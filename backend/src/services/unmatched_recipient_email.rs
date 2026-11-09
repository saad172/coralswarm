//! "Email the unmatched" (chat-per-user plan PR 6,
//! `docs/plans/chat-per-user-ingest-and-in-chat-ask.md` §8 row 6). The spec's
//! step 5: "We email the people in those containers who have not linked
//! yet." Under §5.3 there is no separate "link" act any more — a person
//! creates a CoralSwarm account, their verified address matches, and content
//! starts reaching them. This module is how they hear they should.
//!
//! Four independent jobs, all here rather than split across modules because
//! none is useful without the others:
//!
//!   1. [`claim_notice_slot`] — the persisted, atomically-claimed
//!      per-recipient cooldown (`slack_unmatched_email_notices`, migrations
//!      control `0227`/`0228`/`0229`, CONTROL-PLANE ONLY — see
//!      "Control-plane only, not per-plane" below). §11 correction 4: the
//!      existing in-channel courtesy-notice cooldown
//!      (`connectors::slack::should_send_unmatched_notice`) is in-process,
//!      per-machine, and keyed by CONTAINER — fine for a Slack courtesy
//!      line, wrong for a real inbox (duplicate sends once per machine per
//!      deploy). This is the persisted, per-RECIPIENT replacement. It also
//!      mints (on first insert only) the per-recipient [`unsubscribe/
//!      signup-prefill tokens`](NoticeTokens) jobs 3 and 4 act on, and
//!      REFUSES to claim a recipient job 3 has already suppressed.
//!   2. [`send_nudge_email`] — the Resend send call and its OWN copy matrix
//!      ([`nudge_copy`]), structurally mirroring `routes::org_invites::
//!      render_invite_email_html` (table layout, inline styles, brand fonts
//!      — see that function's own doc comment for why) but never calling
//!      into it: an invite email and an unmatched-recipient nudge are
//!      different messages to different audiences and must not share a
//!      runtime code path just because they share a look. Carries the
//!      one-click `List-Unsubscribe`/`List-Unsubscribe-Post` headers (RFC
//!      8058) and a visible unsubscribe link in the body — see job 3. The
//!      signup link itself carries the OPAQUE prefill token, never the raw
//!      address — see job 4 (CodeRabbit round 1, #1005).
//!   3. [`suppress_by_token`] — the write side of one-click unsubscribe
//!      (`routes::email_unsubscribe`, the ONLY unauthenticated caller of
//!      anything in this module). Resolves the bare token to an account via
//!      the control-plane SECURITY DEFINER `resolve_unmatched_email_notice_
//!      account_id` (migration `0228`, same bootstrap-resolver shape 0086
//!      established for `routes::shares::public_share`'s token lookup), then
//!      sets `suppressed_at` inside a REAL `begin_tenant_system_scoped`
//!      transaction — this table's writes stay exactly as auditable as they
//!      were before this job existed, no exception carved out for the
//!      public route. **Control-plane, for every account, personal or org**
//!      — see "Control-plane only, not per-plane" below; this is not the
//!      `routes::shares::public_share` limitation it used to be modelled on.
//!   4. [`resolve_prefill_token`] — the read side of the signup-prefill link
//!      (`routes::email_prefill`, CodeRabbit round 1, #1005). Same
//!      bootstrap-resolver shape as job 3, keyed on `signup_prefill_token`
//!      instead (migration `0229`): a SEPARATE random value with no shared
//!      lifetime with the unsubscribe token, so a leaked signup link is
//!      never also a working unsubscribe link or vice versa. Returns the
//!      email as a plain read inside the same `begin_tenant_system_scoped`
//!      shape — never a signed/HMAC token embedding the address, which
//!      would be integrity-protected but NOT confidential (see
//!      [`mint_prefill_token`]'s own doc comment).
//!
//! # Control-plane only, not per-plane (correction: an earlier version of
//! this module put the table's use on whichever plane a tick happened to be
//! running for, and routed AROUND the resulting gap at the enqueue instead
//! of closing it)
//!
//! A send-suppression list is bookkeeping about an EMAIL ADDRESS and an
//! ACCOUNT — it carries no `graph_id`, anchors no ocean, and is never read
//! back as content. That puts it in the same category as `oauth_tokens` and
//! `slack_integrations`, both of which are control-only for the identical
//! reason, not a special case invented for this table. The founder's
//! correction that drove this file to its current shape: consumer and org
//! are the SAME PLANE, and a feature that only works for one of them is not
//! done — "an org's rows live in a database this path cannot reach" is not a
//! valid reason to skip a recipient, it is a bug report about which pool the
//! WRITE went to.
//!
//! More importantly: [`suppress_by_token`]'s caller
//! (`routes::email_unsubscribe`) is a PUBLIC, UNAUTHENTICATED route with no
//! `Tenant`/`TenancyDecision` to extract a per-request plane from at all —
//! see that route's own module doc. Control is the ONE plane that route can
//! reach without deciding anything, so it is the only plane a table this
//! route must resolve against can correctly live on, independent of how
//! ocean CONTENT is sharded today or ever gets resharded later. Putting this
//! table on control makes the lookup correct BY CONSTRUCTION, rather than
//! correct only for personal accounts by coincidence (which is what an
//! earlier version of this file was, before the org-tenant recipient's row
//! was moved here from that org's own tenant database).
//!
//! Concretely: [`claim_notice_slot`]'s caller
//! (`workers::slack_unmatched_email::process_job`) now ALWAYS opens its
//! scoped transaction on the shared control pool, never on `t.pool` (the
//! plane-specific pool `workers::tenant_harness::run_multi_tenant` hands a
//! tenant-looped worker for a given tick) — see that worker's own module doc
//! for the corrected wording. `migrations_tenant/0093`/`0095` (this table's
//! former tenant-tree twin) are deleted rather than kept unused: nothing
//! reads or writes this table on the tenant plane anymore, on purpose, and a
//! tenant DB that no longer even defines the table turns any FUTURE regression
//! back to a per-tick pool into a loud "relation does not exist" instead of a
//! silent write to a database `suppress_by_token` can never see.
//!
//! # Copy honesty (contract I5, contract §2 point 4)
//!
//! I5 — "opt-out governs receiving, never capture" — means this email must
//! not read as an opt-IN gate on capture. The channel this person is in is
//! ALREADY being captured by a colleague's agent; that started at the gate
//! event (someone added the agent to the channel) and does not wait for, or
//! depend on, this email. Creating an account is how the recipient gets
//! their OWN copy of what is already being captured — never framed as
//! "capture starts once you sign up" or "ignore this to opt out". See
//! [`nudge_copy`]'s own doc comment for the exact sentence that states this.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use rand::Rng;
use uuid::Uuid;

use crate::AppState;

/// Mint a fresh unsubscribe token: 32 bytes of CSPRNG randomness, base64url
/// (no padding) encoded — the IDENTICAL idiom `routes::shares::
/// generate_share_token` uses for meeting-share tokens, duplicated for the
/// same "this module is its own copy matrix" reason `escape_html`/
/// `host_display` below already give, not a new choice. See migration
/// `0228_slack_unmatched_email_notices_unsubscribe.sql`'s header for why
/// this is a random value stored on the row rather than an HMAC.
fn mint_unsubscribe_token() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

/// Mint a fresh signup-prefill token — same idiom as
/// [`mint_unsubscribe_token`] (32 bytes CSPRNG, base64url no-pad), a
/// SEPARATE random value rather than reusing the unsubscribe token: the two
/// serve different consumers with no reason to share a lifetime, and a
/// leaked prefill link must not also be a working unsubscribe link or vice
/// versa. See migration `0229_slack_unmatched_email_notices_prefill_token.
/// sql`'s header for why this is a random column value and not a signed
/// (JWT/HMAC) token — a signed token is integrity-protected, not
/// confidential, so it would still leak the email to anyone who captures the
/// URL.
fn mint_prefill_token() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

/// How long one (account, provider, email) triple stays quiet after being
/// sent this nudge. Deliberately longer than
/// `connectors::slack::UNMATCHED_NOTICE_COOLDOWN` (6h, calibrated for an
/// in-channel courtesy line nobody is required to read) — an email is a real
/// inbox interruption. Seven days gives someone a full work week to notice
/// and sign up before a second nudge, while still eventually re-reminding
/// someone who missed the first one. See migration
/// `migrations_control/0227_slack_unmatched_email_notices.sql`'s header,
/// which mirrors this exact value in its own comment — if this constant ever
/// changes, that comment goes stale and should be updated alongside it.
///
/// `pub(crate)`: `workers::member_connect_nudge::candidates_for_account`
/// reuses this SAME value (CodeRabbit round 1, CS-436) to exclude a
/// candidate who is already inside an active cooldown from the candidate
/// SELECT itself, not merely from the per-candidate claim — see that
/// function's own doc comment for why a second, independent "7 days"
/// literal in that query would be a silent-drift risk this const exists to
/// prevent.
pub(crate) const COOLDOWN: &str = "7 days";

/// Claim this recipient's notice slot for [`COOLDOWN`], atomically, and
/// return the row's stable [`unsubscribe token`](mint_unsubscribe_token) —
/// `send_nudge_email`'s caller needs it to build the `List-Unsubscribe`
/// header and the in-body link for THIS send.
///
/// **Claim-as-you-report, in one statement** (this PR's brief, §11
/// correction 4): `INSERT ... ON CONFLICT ... DO UPDATE ... WHERE <row is
/// older than the cooldown AND not suppressed> RETURNING id, unsubscribe_
/// token`. Two concurrent callers racing the SAME `(provider, email)` —
/// D4 (`docs/plans/org-onboarding-from-the-invite.md` §3 item 2, CS-427):
/// the key dropped `account_id` here, migration `0230` — cannot both win:
/// Postgres serializes the second INSERT behind the first's row lock on the
/// unique index, and once the first commits, the second's `ON CONFLICT DO
/// UPDATE` re-evaluates its `WHERE` against the NOW-committed row — which
/// fails the "older than the cooldown" test, so the second statement
/// affects ZERO rows and `RETURNING` yields nothing. This is what makes it
/// safe for TWO DIFFERENT accounts (an org's own tick and another org's, or
/// a personal account's) to race a claim for the SAME real person — no
/// longer merely two callers under the SAME account_id, now that the key no
/// longer includes it. `workers::slack_unmatched_email::process_job`'s own
/// `connectors::slack_unmatched_email::select_notice_account` call picks
/// the D4-preferred account BEFORE ever reaching this statement, so this
/// race is a safety net for correctness, not the primary mechanism for
/// choosing WHICH account's copy wins — see that function's own doc
/// comment.
/// `Ok(Some(token))` (a row came back) is the ONLY signal that means "you
/// own this send"; `Ok(None)` means a brand-new insert lost the race a
/// moment earlier, an existing row is still inside its cooldown, or — the
/// case this PR adds — the recipient has unsubscribed. All three mean
/// "someone else already has this, or will, or must never again: do not
/// send."
///
/// **Suppression cannot race a claim** (this PR's brief): `AND suppressed_at
/// IS NULL` is folded into the SAME `WHERE` clause as the cooldown check,
/// not a separate query run before or after it — there is no window between
/// "read suppressed_at" and "claim the slot" for a concurrent [`suppress_by_
/// token`] to land in. A row that is ALREADY suppressed by the time this
/// statement runs never matches the `WHERE`, full stop; a row that becomes
/// suppressed a moment later is caught by the very next call for the same
/// recipient, exactly like the cooldown check it sits beside.
///
/// **The token is minted every call but only ever TAKES on a fresh INSERT
/// (or a legacy NULL row)**: `$5` is a freshly-generated candidate on every
/// invocation, and the `ON CONFLICT DO UPDATE`'s `SET` list assigns
/// `unsubscribe_token = COALESCE(<existing>, <candidate>)` — an existing
/// row's real token always wins over the freshly-discarded candidate, so a
/// token already delivered to an inbox keeps working across every later
/// cooldown renewal. The `COALESCE` exists only to self-heal a row written
/// before this column existed (`unsubscribe_token IS NULL`, migration `0227`
/// predates `0228`): without it, such a row's `RETURNING
/// unsubscribe_token` would be NULL forever, since nothing else ever writes
/// this column after the initial claim.
///
/// **`account_id` is unconditionally overwritten on every renewal**
/// (`account_id = EXCLUDED.account_id`, migration `0230`, D4): unlike the
/// two tokens above, this column is NOT `COALESCE`d against the existing
/// row, because it is observability about who sent the LAST copy, not a
/// value that must survive once set. Since the key dropped `account_id`
/// (0230), the account that wins a later renewal — chosen by `workers::
/// slack_unmatched_email::process_job`'s `connectors::slack_unmatched_
/// email::select_notice_account` — can legitimately differ from whoever
/// claimed the row before (an org's roster later comes to hold the
/// recipient's verified address, say); this column should reflect the
/// account whose send this row now actually represents, never a stale one.
///
/// Must run inside the caller's own `begin_tenant_system_scoped(control_pool,
/// account_id)` transaction/connection (same convention as every other write
/// this PR's worker makes) — `executor` must be a CONTROL-plane connection or
/// transaction, never a tenant pool's: this table has no tenant-tree twin
/// (see the module doc's "Control-plane only, not per-plane" section), so a
/// tenant executor would simply fail with "relation does not exist" rather
/// than silently writing somewhere `suppress_by_token` can never look. This
/// table's RLS policy reads exactly the GUCs that call sets (see the
/// migration's own header).
/// The two per-recipient tokens [`claim_notice_slot`] mints/returns —
/// independent random values, independent lifetimes (see [`mint_prefill_
/// token`]'s own doc comment for why they are not the same value).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NoticeTokens {
    pub(crate) unsubscribe_token: String,
    pub(crate) prefill_token: String,
}

pub(crate) async fn claim_notice_slot(
    executor: impl sqlx::PgExecutor<'_>,
    account_id: Uuid,
    provider: &str,
    workspace_id: &str,
    channel_id: &str,
    email: &str,
) -> Result<Option<NoticeTokens>, sqlx::Error> {
    let candidate_unsub_token = mint_unsubscribe_token();
    let candidate_prefill_token = mint_prefill_token();
    let claimed: Option<(Uuid, String, String)> = sqlx::query_as(&format!(
        "INSERT INTO slack_unmatched_email_notices \
             (account_id, provider, email, workspace_id, channel_id, last_notified_at, \
              unsubscribe_token, signup_prefill_token) \
         VALUES ($1, $7, $2, $3, $4, now(), $5, $6) \
         ON CONFLICT (provider, email) DO UPDATE \
             SET account_id = EXCLUDED.account_id, \
                 last_notified_at = now(), \
                 workspace_id = EXCLUDED.workspace_id, \
                 channel_id = EXCLUDED.channel_id, \
                 unsubscribe_token = COALESCE( \
                     slack_unmatched_email_notices.unsubscribe_token, EXCLUDED.unsubscribe_token), \
                 signup_prefill_token = COALESCE( \
                     slack_unmatched_email_notices.signup_prefill_token, EXCLUDED.signup_prefill_token) \
             WHERE slack_unmatched_email_notices.last_notified_at < now() - INTERVAL '{COOLDOWN}' \
               AND slack_unmatched_email_notices.suppressed_at IS NULL \
         RETURNING id, unsubscribe_token, signup_prefill_token"
    ))
    .bind(account_id)
    .bind(email)
    .bind(workspace_id)
    .bind(channel_id)
    .bind(&candidate_unsub_token)
    .bind(&candidate_prefill_token)
    .bind(provider)
    .fetch_optional(executor)
    .await?;

    Ok(claimed.map(|(_, unsubscribe_token, prefill_token)| NoticeTokens {
        unsubscribe_token,
        prefill_token,
    }))
}

/// This email's ENTIRE dynamic copy — subject (plain text) and heading
/// (HTML, one italic brand-colored word, matching `org_invites::invite_copy`'s
/// own convention). No org/inviter name parameter: unlike an admin-issued
/// org invite, nobody chose to invite this specific person — their address
/// simply appeared in a Slack roster the agent already watches. Naming a
/// person as "inviter" here would overstate what actually happened.
fn nudge_copy() -> (&'static str, String) {
    let em = |word: &str| format!("<em style=\"font-style:italic;color:#D14A38\">{word}</em>");
    (
        "Your Slack conversations are already landing in CoralSwarm",
        format!("You're already being {}", em("captured")),
    )
}

/// Minimal HTML entity escaping — identical rule to `org_invites::
/// escape_html`, duplicated rather than imported: this module is deliberately
/// its OWN copy matrix (this PR's brief), and a shared escaping helper this
/// trivial is not worth coupling two independent email templates over.
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

/// `https://acme.coralswarm.example` -> `acme.coralswarm.example` — identical rule to
/// `org_invites::host_display`, duplicated for the same "own copy matrix"
/// reason `escape_html` above gives.
fn host_display(web_base: &str) -> &str {
    let no_scheme = web_base
        .strip_prefix("https://")
        .or_else(|| web_base.strip_prefix("http://"))
        .unwrap_or(web_base);
    no_scheme.split('/').next().unwrap_or(no_scheme)
}

const FONT_SANS: &str = "Geist,-apple-system,'Segoe UI',system-ui,sans-serif";
const FONT_SERIF: &str = "'Instrument Serif',Georgia,'Times New Roman',serif";
const FONT_MONO: &str = "'JetBrains Mono',ui-monospace,Consolas,monospace";

/// Render the nudge email body. Deliberately the SAME structural shell as
/// `org_invites::render_invite_email_html` (table layout, inline styles, no
/// webfont loads, cream document palette) — see that function's own doc
/// comment for why email clients force these constraints — but its OWN copy
/// end to end: this is not a parameterised call into that function (this
/// PR's brief calls for a second copy matrix, not a shared one).
///
/// The mono provenance line is where I5 honesty (this module's own doc
/// comment) actually lives in the rendered text: it states PLAINLY that the
/// channel is captured regardless of whether this person ever signs in, so
/// the email cannot be misread as "capture is gated on you" or "ignoring
/// this opts you out."
///
/// `unsubscribe_url` renders as a plain visible link in the footer — the
/// `List-Unsubscribe` HEADER `send_nudge_email` also sets is invisible in
/// every mail client's rendered view, and Google/Yahoo's bulk-sender rules
/// (and plain "clear and conspicuous" practice) expect a reader to be able
/// to find it without inspecting headers. Deliberately NOT styled as a
/// button/CTA the way the sign-up link above it is — an unsubscribe link
/// that looks as inviting as the primary action would undercut I5 by
/// implying leaving is the intended next step, when the email's whole point
/// is that capture continues either way.
fn render_nudge_email_html(to_email: &str, join_url: &str, web_base: &str, unsubscribe_url: &str) -> String {
    let email_esc = escape_html(to_email);
    let url_esc = escape_html(join_url);
    let host_esc = escape_html(host_display(web_base));
    let unsubscribe_esc = escape_html(unsubscribe_url);
    let (_, heading) = nudge_copy();

    format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width\"></head>\
<body style=\"margin:0;padding:0;background:#F2EBE0\">\
<div style=\"display:none;max-height:0;overflow:hidden;mso-hide:all\">A colleague's CoralSwarm agent is already saving this conversation — here's how you get your own copy.</div>\
<table role=\"presentation\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" bgcolor=\"#F2EBE0\"><tr><td align=\"center\" style=\"padding:40px 16px\">\
<table role=\"presentation\" width=\"560\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" style=\"width:560px;max-width:100%\">\
<tr><td bgcolor=\"#FBF7F2\" style=\"border:1px solid #D9CCB8;border-radius:12px;padding:34px 36px;font-family:{FONT_SANS};color:#1A1410\">\
<div style=\"margin-bottom:26px;font-weight:600;font-size:19px;letter-spacing:-0.045em;color:#1A1410;line-height:1\">coralswarm</div>\
<div style=\"font-family:{FONT_MONO};font-size:10px;text-transform:uppercase;letter-spacing:0.14em;color:#8A7B6E;margin-bottom:12px\">a colleague already connected coralswarm</div>\
<h1 style=\"margin:0 0 14px 0;font-family:{FONT_SERIF};font-size:30px;font-weight:400;line-height:1.2;color:#1A1410\">{heading}</h1>\
<p style=\"margin:0 0 16px 0;font-size:14px;line-height:1.6;color:#4A3F36\">Someone in a Slack channel you're in connected their team's CoralSwarm agent, and it is now saving that channel's conversation into your team's shared memory.</p>\
<p style=\"margin:0 0 22px 0;font-size:14px;line-height:1.6;color:#4A3F36\">Create a CoralSwarm account with this address — <span style=\"font-family:{FONT_MONO};font-size:13px;color:#1A1410\">{email_esc}</span> — and everything already captured starts landing in your own private ocean, cited and searchable, the moment you sign in.</p>\
<table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" style=\"margin-bottom:12px\"><tr>\
<td bgcolor=\"#F77764\" style=\"border-radius:8px\">\
<a href=\"{url_esc}\" style=\"display:inline-block;padding:11px 22px;font-family:{FONT_SANS};font-size:15px;font-weight:500;color:#FFFFFF;text-decoration:none\">Create your CoralSwarm account</a>\
</td></tr></table>\
<p style=\"margin:0 0 26px 0;font-family:{FONT_MONO};font-size:10.5px;line-height:1.6;color:#8A7B6E\">signs you in at {host_esc} · email code, no password</p>\
<table role=\"presentation\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\"><tr>\
<td style=\"border-top:1px solid #D9CCB8;padding-top:16px\">\
<p style=\"margin:0 0 10px 0;font-family:{FONT_MONO};font-size:9.5px;line-height:1.7;color:#8A7B6E\">This channel is captured whether or not you create an account — signing in is how YOU get a copy of it, not a way to stop it being saved.<br>Not expecting this? Ignore it — nothing else happens until you sign in.</p>\
<p style=\"margin:0;font-family:{FONT_MONO};font-size:9.5px;line-height:1.7\"><a href=\"{unsubscribe_esc}\" style=\"color:#8A7B6E;text-decoration:underline\">Unsubscribe from this reminder</a></p>\
</td></tr></table>\
</td></tr>\
<tr><td align=\"center\" style=\"padding-top:14px\">\
<p style=\"margin:0;font-family:{FONT_MONO};font-size:9.5px;color:#8A7B6E\">coralswarm · your team's living memory</p>\
</td></tr></table>\
</td></tr></table></body></html>"
    )
}

fn resend_base_url() -> String {
    std::env::var("RESEND_API_BASE_URL").unwrap_or_else(|_| "https://api.resend.com".to_string())
}

/// Serializes every lib test that points `RESEND_API_BASE_URL` at a local
/// mock. Each call site used to keep its own lock, so two tests in one
/// `cargo test --lib` run could interleave: one test's POST hit the other's
/// mock (2xx, so the send reported success) while its own capture stayed
/// empty.
#[cfg(test)]
pub(crate) static RESEND_BASE_URL_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// The domain half of an email address, for logging — never the full
/// address (same PII discipline as `org_invites::email_domain`, duplicated
/// for the same "own copy matrix, no coupling" reason as `escape_html`
/// above).
fn email_domain(email: &str) -> &str {
    email.split('@').nth(1).unwrap_or(email)
}

/// Send one nudge email via Resend. Best-effort: any failure (missing key,
/// non-2xx, transport error, or a hang past 10s) returns `false` and never
/// panics or propagates — the caller (`workers::slack_unmatched_email`) has
/// ALREADY claimed this recipient's cooldown slot by the time this runs (see
/// [`claim_notice_slot`]'s own doc comment on why the claim happens first),
/// so a failed send here is a lost nudge, not a double-send risk: the next
/// roster walk inside the cooldown window will not re-claim, but the one
/// after it will, same as any other best-effort notification in this crate.
///
/// `unsubscribe_token` is [`claim_notice_slot`]'s return value for THIS
/// recipient — the caller is expected to pass it straight through without
/// re-deriving or caching it, since the token is only ever authoritative
/// coming off that claim.
///
/// # One-click unsubscribe (RFC 8058)
/// Sets BOTH the `List-Unsubscribe` header (the mailto/URL a mail client's
/// own "Unsubscribe" affordance reads) and `List-Unsubscribe-Post:
/// List-Unsubscribe=One-Click`, which is what tells a compliant client it
/// may fire the unsubscribe with a bare, unauthenticated POST and no
/// confirmation UI — RFC 8058 §3.1 requires BOTH headers together; the Post
/// header alone is meaningless without the URL the first one supplies, and
/// the URL alone re-adds a confirmation click Google/Yahoo's bulk-sender
/// rules exist to remove. Resend does not expose these as first-class send
/// options — the `headers` object on the send payload is Resend's documented
/// escape hatch for arbitrary custom headers, so that is what carries both.
/// [`render_nudge_email_html`] additionally renders the SAME URL as a
/// visible in-body link — the header is invisible in a rendered message, and
/// "clear and conspicuous" opt-out is a Google/Yahoo requirement in its own
/// right, not satisfied by the header alone.
pub(crate) async fn send_nudge_email(
    state: &AppState,
    to_email: &str,
    web_base: &str,
    unsubscribe_token: &str,
    prefill_token: &str,
) -> bool {
    let Some(resend_key) = state.resend_api_key.clone() else {
        return false;
    };
    let (subject, _) = nudge_copy();
    // `/signup`, NOT `/onboarding/join`.
    //
    // `/join` is the ORG-INVITE landing page and it is sign-IN only: it drives
    // `signIn.emailCode.sendCode/verifyCode/finalize` and has no `signUp` path
    // at all. That works for an invitee because `routes::org_invites::
    // create_invite` PRE-CREATES their Clerk user before the invite email goes
    // out. The people this nudge reaches have no Clerk user — nobody invited
    // them; they are simply in a Slack channel the agent is also in — so
    // `sendCode` would find no account and the link would be a dead end for
    // every single recipient. `web/app/signup/page.tsx`'s own comment states
    // the split from the other side: "an org is still joined by invitation,
    // through /onboarding/join, which never routes here."
    //
    // `/signup` renders Clerk's `<SignUp>`, which is the self-serve path these
    // recipients actually need. CodeRabbit round 1 (#1005, Security &
    // Privacy): an earlier version of this carried the raw address as
    // `?email=`, which browser history, proxy access logs, analytics, and
    // `Referer` headers can all retain. `?pt=` is an opaque, unguessable,
    // DB-backed token instead (`claim_notice_slot`'s `signup_prefill_token`,
    // migration `0229`) — `web/app/signup/page.tsx` resolves it against
    // `routes::email_prefill` (a fetch, not a redirect) before rendering the
    // form, so the address itself never appears in any URL at all, not even
    // one only the recipient's own browser sees.
    let join_url = format!("{web_base}/signup?pt={}", urlencoding::encode(prefill_token));
    // `?t={token}` — the exact query param `routes::email_unsubscribe`
    // expects (`UnsubscribeQuery`). `state.api_base_url()`, NOT `web_base`
    // (CodeRabbit round 1, #1005, Functional Correctness): `/v1/email/
    // unsubscribe` is a BACKEND route, mounted on the API origin
    // (`AppState::api_base_url`, `https://{apex}`) — `web_base` is the WEB
    // APP's own origin (`https://app.{web_apex}`, for personal and org alike
    // now that org subdomains are retired), a different host the backend does not
    // listen on at all. An earlier version of this comment argued the choice
    // of host did not matter because "the unsubscribe route resolves against
    // the shared control pool regardless of which host served the link" —
    // that conflates which DATABASE the route reads once a request reaches
    // it with which HOST answers the HTTP request in the first place; the
    // web app's own origin never routes `/v1/*` to this backend at all, so
    // every link built with `web_base` 404s before the pool question is ever
    // reached. The token is already URL-safe base64
    // ([`mint_unsubscribe_token`]), so no additional percent-encoding is
    // needed the way `to_email` above needs.
    let unsubscribe_url = format!(
        "{}/v1/email/unsubscribe?t={unsubscribe_token}",
        state.api_base_url()
    );
    let payload = serde_json::json!({
        "from": "coralswarm <person@example.com>",
        "to": [to_email],
        "subject": subject,
        "html": render_nudge_email_html(to_email, &join_url, web_base, &unsubscribe_url),
        // RFC 8058 one-click unsubscribe — see this function's own doc
        // comment for why both headers are required together and why they
        // land here (Resend's documented custom-header escape hatch) rather
        // than as first-class send options.
        "headers": {
            "List-Unsubscribe": format!("<{unsubscribe_url}>"),
            "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
        },
    });
    let url = format!("{}/emails", resend_base_url());
    let send = state.http_client.post(&url).bearer_auth(&resend_key).json(&payload).send();
    match tokio::time::timeout(std::time::Duration::from_secs(10), send).await {
        Ok(Ok(resp)) if resp.status().is_success() => true,
        Ok(Ok(resp)) => {
            tracing::warn!(
                status = %resp.status(), email_domain = email_domain(to_email),
                "unmatched_recipient_email: Resend returned non-success for nudge email"
            );
            false
        }
        Ok(Err(e)) => {
            tracing::warn!(
                error = %e, email_domain = email_domain(to_email),
                "unmatched_recipient_email: failed to send nudge email via Resend"
            );
            false
        }
        Err(_) => {
            tracing::warn!(
                email_domain = email_domain(to_email),
                "unmatched_recipient_email: Resend send timed out"
            );
            false
        }
    }
}

/// Mark the recipient behind `token` as suppressed. `Ok(true)` means a real
/// row was found and updated (or already had `suppressed_at` set — see
/// below); `Ok(false)` means the token matched no row at all. **The two
/// outcomes must render an IDENTICAL response to the caller** —
/// `routes::email_unsubscribe`'s whole enumeration-resistance contract rests
/// on that, exactly like `routes::shares`'s generic 404 for an unknown vs.
/// revoked share token — this function's boolean exists for LOGGING only,
/// never to select a different HTTP response.
///
/// # Two-step bootstrap, same shape as `routes::shares::resolve_share_by_
/// token` (org-tenancy B.3's pattern, not a new one)
/// 1. [`resolve_unmatched_email_notice_account_id`] (migration `0228`, a
///    SECURITY DEFINER `SELECT`) discovers the owning account from the bare
///    token — the chicken-and-egg every unauthenticated-capability-token
///    route in this crate has: no account is known yet, so no
///    `begin_tenant_system_scoped` scope can be opened at all until one is
///    found this way. `None` (no row matches the token) short-circuits to
///    `Ok(false)` — same generic outcome as any other refusal on this path.
/// 2. Once the account is known, the actual write runs inside a REAL
///    `begin_tenant_system_scoped(control_pool, account_id)` transaction — this
///    table's OWN invariant (migration `0227`'s header: "only the tenant
///    SYSTEM actor" writes it) holds for this write exactly as it does for
///    every other one, unlike a design that let the bootstrap resolver
///    itself perform the UPDATE (which would need to be `VOLATILE` and
///    would bypass this table's RLS-backed system-only posture for a WRITE,
///    not just the narrow bootstrap READ 0086's own pattern is scoped to).
///
/// **Idempotent by construction**: `COALESCE(suppressed_at, now())` means a
/// second (or hundredth) POST against the same token is a no-op that still
/// reports success — a mail provider retrying a one-click POST it never saw
/// the response to must never see a different outcome the second time.
///
/// **`control_pool` is not a limitation here — it is the whole point.**
/// `routes::email_unsubscribe` (this function's only caller) is a PUBLIC,
/// unauthenticated route with no `Tenant`/`TenancyDecision` to derive a plane
/// from at all; control is the one plane it can resolve against without
/// deciding anything. Unlike `routes::shares::public_share`'s token lookup
/// (a genuinely tenant-tree table, so an org-minted share really does live
/// somewhere this route cannot reach), this table has NO tenant-tree twin at
/// all (see the module doc's "Control-plane only, not per-plane" section) —
/// every recipient's row, personal account or org account alike, lives here
/// because [`claim_notice_slot`]'s caller writes it here unconditionally.
/// `resolve_unmatched_email_notice_account_id` therefore resolves an org
/// account's token exactly as completely as a personal one's; there is no
/// remaining plane this function or its caller needs to reach.
pub(crate) async fn suppress_by_token(
    control_pool: &sqlx::PgPool,
    token: &str,
) -> Result<bool, String> {
    let account_id: Option<Uuid> =
        sqlx::query_scalar("SELECT resolve_unmatched_email_notice_account_id($1)")
            .bind(token)
            .fetch_one(control_pool)
            .await
            .map_err(|e| format!("suppress_by_token: resolve query failed: {e}"))?;
    let Some(account_id) = account_id else {
        return Ok(false);
    };

    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(control_pool, account_id)
        .await
        .map_err(|e| format!("suppress_by_token: begin tx: {e}"))?;
    sqlx::query(
        "UPDATE slack_unmatched_email_notices \
             SET suppressed_at = COALESCE(suppressed_at, now()) \
         WHERE unsubscribe_token = $1",
    )
    .bind(token)
    .execute(&mut *tx)
    .await
    .map_err(|e| format!("suppress_by_token: update failed: {e}"))?;
    tx.commit().await.map_err(|e| format!("suppress_by_token: commit: {e}"))?;

    Ok(true)
}

/// Resolve a signup-prefill token to the email address it was minted for
/// (CodeRabbit round 1, #1005) — `routes::email_prefill`'s only caller.
///
/// Identical two-step shape to [`suppress_by_token`], adapted from a write to
/// a read: bootstrap the `account_id` via the narrow SECURITY DEFINER
/// resolver (migration `0229`, never itself returning a content column),
/// then read the actual `email` inside a REAL
/// `begin_tenant_system_scoped(control_pool, account_id)` transaction — this
/// table's RLS policy applies to the read exactly as it does to every write,
/// no exception carved out for the public route. `Ok(None)` for an unknown
/// or already-expired-by-deletion token is the SAME generic outcome
/// `routes::email_prefill` returns regardless of cause — this is a public
/// route with no session, so the response must not distinguish "no such
/// token" from any other reason nothing came back (the identical
/// enumeration-resistance discipline `routes::email_unsubscribe`'s own
/// module doc states).
pub(crate) async fn resolve_prefill_token(
    control_pool: &sqlx::PgPool,
    token: &str,
) -> Result<Option<String>, String> {
    let account_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT resolve_unmatched_email_notice_account_id_by_prefill_token($1)",
    )
    .bind(token)
    .fetch_one(crate::tenancy::scope::request_read(control_pool))
    .await
    .map_err(|e| format!("resolve_prefill_token: resolve query failed: {e}"))?;
    let Some(account_id) = account_id else {
        return Ok(None);
    };

    let mut tx = crate::tenancy::scope::begin_request_system_read(control_pool, account_id)
        .await
        .map_err(|e| format!("resolve_prefill_token: begin tx: {e}"))?;
    let email: Option<String> = sqlx::query_scalar(
        "SELECT email FROM slack_unmatched_email_notices WHERE signup_prefill_token = $1",
    )
    .bind(token)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| format!("resolve_prefill_token: select failed: {e}"))?;
    tx.rollback().await.ok();

    Ok(email)
}

// ─────────────────────────────────────────────────────────────────────────
// The member-connect nudge (CS-436, org-onboarding plan §3 item 49, D21)
// ─────────────────────────────────────────────────────────────────────────
//
// D21 retires the org install: every member connects each platform with
// their OWN grant, so an approved member who never connects is the new
// silent failure this table must catch (the third audience the plan's §3
// table adds — see `docs/plans/org-onboarding-from-the-invite.md` ~line
// 1237). This is the SAME table, the SAME `(provider, email)` cooldown key
// (D4, migration `0230`), and the SAME `claim-as-you-report` atomicity as
// [`claim_notice_slot`] above — deliberately not a new table — just a new
// `provider` value (`'member_connect'`, no CHECK constraint on that column,
// migration `0227`). `workspace_id`/`channel_id` are genuinely inapplicable
// here (there is no Slack/Teams/Chat container this recipient was
// discovered through — they are found by walking `account_members`, not a
// roster), so migration `0234` drops their `NOT NULL` and every row this
// provider writes carries `NULL` for both.

/// The provider value this nudge claims/renews under — see the section doc
/// above for why it is a new value on the SAME table rather than a new one.
pub(crate) const MEMBER_CONNECT_PROVIDER: &str = "member_connect";

/// "Same suppression list as the parent plan's PR 6" (item 49's own words)
/// means: an email that unsubscribed from EITHER nudge must never receive
/// THIS one, not merely a re-check of this provider's own row. `claim_
/// member_connect_notice_slot`'s `ON CONFLICT ... WHERE ... suppressed_at IS
/// NULL` only ever inspects the `member_connect` row for this email (the
/// `(provider, email)` key is provider-specific by construction, migration
/// `0230`) — a person who unsubscribed from the Slack-unmatched nudge under
/// `provider = 'slack'` has a DIFFERENT row, which that `WHERE` clause never
/// reads. This is the explicit any-provider check the claim alone cannot
/// give: called BEFORE [`claim_member_connect_notice_slot`], never as a
/// substitute for it (the claim's own same-provider suppression check still
/// matters for correctness once this call has already said "not
/// suppressed elsewhere").
pub(crate) async fn is_suppressed_any_provider(
    executor: impl sqlx::PgExecutor<'_>,
    email: &str,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT EXISTS ( \
             SELECT 1 FROM slack_unmatched_email_notices \
             WHERE email = $1 AND suppressed_at IS NOT NULL \
         )",
    )
    .bind(email)
    .fetch_one(executor)
    .await
}

/// [`claim_notice_slot`]'s member-connect twin — identical shape (same
/// [`COOLDOWN`], same claim-as-you-report `INSERT ... ON CONFLICT (provider,
/// email) DO UPDATE ... WHERE <older than cooldown> AND <not suppressed>
/// RETURNING`, same [`NoticeTokens`] return contract), a SIBLING function
/// rather than an overload of `claim_notice_slot` because the column shapes
/// genuinely differ: no `workspace_id`/`channel_id` to bind or refresh (see
/// the section doc above), and `provider` is a literal, not a bind
/// parameter — `'member_connect'` is baked into the SQL text rather than
/// passed in, the same way `claim_notice_slot` bakes in `'slack'`.
///
/// `signup_prefill_token` is still minted and stored on every fresh INSERT
/// (kept for schema-shape symmetry with every other row this table holds —
/// `resolve_prefill_token`'s SELECT has no provider filter, so leaving the
/// column populated costs nothing), but this nudge's own send path
/// ([`send_connect_nudge_email`]) never reads it back: the recipient here is
/// already a CoralSwarm user with a verified `users` row (an
/// `account_members` member, by construction — see `workers::
/// member_connect_nudge`'s candidate predicate), so the email's call to
/// action is a plain link to the connectors page, not a `/signup?pt=`
/// prefill link built for someone who has no account at all.
///
/// Caller contract identical to `claim_notice_slot`: must run inside the
/// caller's own control-plane transaction/connection — see [`claim_notice_
/// slot`]'s own doc comment for the full "control-plane only, no tenant-tree
/// twin" reasoning, which applies here unchanged.
pub(crate) async fn claim_member_connect_notice_slot(
    executor: impl sqlx::PgExecutor<'_>,
    account_id: Uuid,
    email: &str,
) -> Result<Option<NoticeTokens>, sqlx::Error> {
    let candidate_unsub_token = mint_unsubscribe_token();
    let candidate_prefill_token = mint_prefill_token();
    let claimed: Option<(Uuid, String, String)> = sqlx::query_as(&format!(
        "INSERT INTO slack_unmatched_email_notices \
             (account_id, provider, email, workspace_id, channel_id, last_notified_at, \
              unsubscribe_token, signup_prefill_token) \
         VALUES ($1, '{MEMBER_CONNECT_PROVIDER}', $2, NULL, NULL, now(), $3, $4) \
         ON CONFLICT (provider, email) DO UPDATE \
             SET account_id = EXCLUDED.account_id, \
                 last_notified_at = now(), \
                 unsubscribe_token = COALESCE( \
                     slack_unmatched_email_notices.unsubscribe_token, EXCLUDED.unsubscribe_token), \
                 signup_prefill_token = COALESCE( \
                     slack_unmatched_email_notices.signup_prefill_token, EXCLUDED.signup_prefill_token) \
             WHERE slack_unmatched_email_notices.last_notified_at < now() - INTERVAL '{COOLDOWN}' \
               AND slack_unmatched_email_notices.suppressed_at IS NULL \
         RETURNING id, unsubscribe_token, signup_prefill_token"
    ))
    .bind(account_id)
    .bind(email)
    .bind(&candidate_unsub_token)
    .bind(&candidate_prefill_token)
    .fetch_optional(executor)
    .await?;

    Ok(claimed.map(|(_, unsubscribe_token, prefill_token)| NoticeTokens {
        unsubscribe_token,
        prefill_token,
    }))
}

/// This email's dynamic copy — same `(subject, heading_html)` shape as
/// [`nudge_copy`], one italic brand-colored word max (the brand skill's
/// email rule). No org-name parameter: `accounts` carries no display-name
/// column (`clerk_org_id` only — the name lives in Clerk), and reaching out
/// to the Clerk API from inside a control-plane sweep for copy alone would
/// make every send depend on an external call this worker's own tests
/// cannot exercise without a Clerk mock. The CTA is the one-line "connect"
/// D6 calls for, and deliberately names no specific platform (Slack/Teams
/// are still `AccountWide`, not yet a per-member connect action — see
/// `workers::member_connect_nudge`'s own module doc) — "your tools", plural,
/// covers whatever this member can actually connect today.
fn connect_nudge_copy() -> (&'static str, String) {
    let em = |word: &str| format!("<em style=\"font-style:italic;color:#D14A38\">{word}</em>");
    (
        "Connect your tools on coralswarm",
        format!("Time to {}", em("connect")),
    )
}

/// Render the connect-nudge email body. Same structural shell as
/// [`render_nudge_email_html`]/`org_invites::render_invite_email_html`
/// (cream document palette, inline styles, no webfont loads — see either's
/// own doc comment for why email clients force these constraints), its OWN
/// copy end to end.
///
/// **Copy honesty (house rule, this story's brief):** this recipient IS a
/// member — approval already granted them a place in the org — so the copy
/// says exactly what is true and nothing more: they "haven't connected a
/// tool yet", never "have no data" or "an empty ocean" (a member with team-
/// ocean access may already see content other members promoted; only their
/// OWN capture is what connecting adds — see D21, `docs/plans/
/// org-onboarding-from-the-invite.md`). The footer is also deliberately NOT
/// [`render_nudge_email_html`]'s "you're already being captured regardless"
/// line — that sentence would be FALSE for this audience: under D21 nothing
/// is captured on this member's behalf until they connect, full stop.
fn render_connect_nudge_email_html(
    to_email: &str,
    connect_url: &str,
    web_base: &str,
    unsubscribe_url: &str,
) -> String {
    let email_esc = escape_html(to_email);
    let url_esc = escape_html(connect_url);
    let host_esc = escape_html(host_display(web_base));
    let unsubscribe_esc = escape_html(unsubscribe_url);
    let (_, heading) = connect_nudge_copy();

    format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width\"></head>\
<body style=\"margin:0;padding:0;background:#F2EBE0\">\
<div style=\"display:none;max-height:0;overflow:hidden;mso-hide:all\">You're a member of your team's coralswarm workspace — connect a tool to start your own capture.</div>\
<table role=\"presentation\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" bgcolor=\"#F2EBE0\"><tr><td align=\"center\" style=\"padding:40px 16px\">\
<table role=\"presentation\" width=\"560\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" style=\"width:560px;max-width:100%\">\
<tr><td bgcolor=\"#FBF7F2\" style=\"border:1px solid #D9CCB8;border-radius:12px;padding:34px 36px;font-family:{FONT_SANS};color:#1A1410\">\
<div style=\"margin-bottom:26px;font-weight:600;font-size:19px;letter-spacing:-0.045em;color:#1A1410;line-height:1\">coralswarm</div>\
<div style=\"font-family:{FONT_MONO};font-size:10px;text-transform:uppercase;letter-spacing:0.14em;color:#8A7B6E;margin-bottom:12px\">you're already a member</div>\
<h1 style=\"margin:0 0 14px 0;font-family:{FONT_SERIF};font-size:30px;font-weight:400;line-height:1.2;color:#1A1410\">{heading}</h1>\
<p style=\"margin:0 0 16px 0;font-size:14px;line-height:1.6;color:#4A3F36\">You're in — but you haven't connected a tool yet, so nothing you do is landing in your own ocean.</p>\
<p style=\"margin:0 0 22px 0;font-size:14px;line-height:1.6;color:#4A3F36\">Connecting takes under a minute with your own account (<span style=\"font-family:{FONT_MONO};font-size:13px;color:#1A1410\">{email_esc}</span>) and starts your own copy of what you capture — cited, searchable, and yours.</p>\
<table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" style=\"margin-bottom:12px\"><tr>\
<td bgcolor=\"#F77764\" style=\"border-radius:8px\">\
<a href=\"{url_esc}\" style=\"display:inline-block;padding:11px 22px;font-family:{FONT_SANS};font-size:15px;font-weight:500;color:#FFFFFF;text-decoration:none\">Connect your tools</a>\
</td></tr></table>\
<p style=\"margin:0 0 26px 0;font-family:{FONT_MONO};font-size:10.5px;line-height:1.6;color:#8A7B6E\">opens at {host_esc} · sign in with your existing account</p>\
<table role=\"presentation\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\"><tr>\
<td style=\"border-top:1px solid #D9CCB8;padding-top:16px\">\
<p style=\"margin:0 0 10px 0;font-family:{FONT_MONO};font-size:9.5px;line-height:1.7;color:#8A7B6E\">Turning off this reminder does not remove you from your workspace or change what you can see.</p>\
<p style=\"margin:0;font-family:{FONT_MONO};font-size:9.5px;line-height:1.7\"><a href=\"{unsubscribe_esc}\" style=\"color:#8A7B6E;text-decoration:underline\">Turn off this reminder</a></p>\
</td></tr></table>\
</td></tr>\
<tr><td align=\"center\" style=\"padding-top:14px\">\
<p style=\"margin:0;font-family:{FONT_MONO};font-size:9.5px;color:#8A7B6E\">coralswarm · your team's living memory</p>\
</td></tr></table>\
</td></tr></table></body></html>"
    )
}

/// Send one connect-nudge email via Resend — same best-effort contract as
/// [`send_nudge_email`] (any failure returns `false`, never panics or
/// propagates) with ONE deliberate difference in how the caller must use
/// it: see `workers::member_connect_nudge::tick`'s own doc comment for why
/// THIS send must complete (or fail) BEFORE the claim transaction commits,
/// reversing `workers::slack_unmatched_email::process_job`'s "claim first,
/// in its own committed transaction, then send" order — a failed send here
/// must not burn this recipient's cooldown window the way a failed Slack
/// nudge send does today.
///
/// `unsubscribe_token` is [`claim_member_connect_notice_slot`]'s return
/// value for THIS recipient, passed straight through exactly like [`send_
/// nudge_email`]'s own parameter of the same name.
pub(crate) async fn send_connect_nudge_email(
    state: &AppState,
    to_email: &str,
    web_base: &str,
    unsubscribe_token: &str,
) -> bool {
    let Some(resend_key) = state.resend_api_key.clone() else {
        return false;
    };
    let (subject, _) = connect_nudge_copy();
    let connect_url = format!("{web_base}/settings/organization/connectors");
    let unsubscribe_url = format!(
        "{}/v1/email/unsubscribe?t={unsubscribe_token}",
        state.api_base_url()
    );
    let payload = serde_json::json!({
        "from": "coralswarm <person@example.com>",
        "to": [to_email],
        "subject": subject,
        "html": render_connect_nudge_email_html(to_email, &connect_url, web_base, &unsubscribe_url),
        "headers": {
            "List-Unsubscribe": format!("<{unsubscribe_url}>"),
            "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
        },
    });
    let url = format!("{}/emails", resend_base_url());
    let send = state.http_client.post(&url).bearer_auth(&resend_key).json(&payload).send();
    match tokio::time::timeout(std::time::Duration::from_secs(10), send).await {
        Ok(Ok(resp)) if resp.status().is_success() => true,
        Ok(Ok(resp)) => {
            tracing::warn!(
                status = %resp.status(), email_domain = email_domain(to_email),
                "unmatched_recipient_email: Resend returned non-success for connect-nudge email"
            );
            false
        }
        Ok(Err(e)) => {
            tracing::warn!(
                error = %e, email_domain = email_domain(to_email),
                "unmatched_recipient_email: failed to send connect-nudge email via Resend"
            );
            false
        }
        Err(_) => {
            tracing::warn!(
                email_domain = email_domain(to_email),
                "unmatched_recipient_email: Resend send timed out for connect-nudge email"
            );
            false
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// The "join this org" email (CS-440, org-onboarding plan §3 item 29, D19 as
// amended by D21)
// ─────────────────────────────────────────────────────────────────────────
//
// A THIRD provider value on the SAME `slack_unmatched_email_notices` table
// — the identical "same table, new provider value" choice `MEMBER_CONNECT_
// PROVIDER`'s own section above already made, for the identical reason:
// this is send bookkeeping with no ocean anchor, this audience needs the
// SAME `(provider, email)` D4 cooldown and suppression machinery
// (migration `0230`), and `provider` carries no `CHECK` constraint
// (migration `0227`) — a new value costs nothing here. This table's claim
// still gives the D4 cooldown and the unsubscribe/suppression mechanism
// (`unsubscribe_token`, `suppressed_at`) — see [`claim_join_notice_slot`]
// below. It no longer carries the CLICK token itself — see the next
// section for why that changed.
//
// # The click token: a signed JWT, not the notices row — CORRECTED
// mid-implementation (CS-440, coordination with CS-439 / PR #1023)
//
// The FIRST version of this section reused this table's existing `signup_
// prefill_token` column (a random, unguessable, DB-backed value with an
// established bootstrap-resolver shape, migration `0229`'s header) rather
// than a signed payload — reasoning: every fact the click needs already
// sits on the notices row, so a second, parallel token design would
// duplicate data for no new capability, and would need its own secret-
// management story a random DB-backed value never has.
//
// That reasoning held for the BACKEND's own needs, but missed a THIRD
// consumer: CS-439's `/join/[org]` landing page (PR #1023) renders BEFORE
// the requester clicks "ask to join" — at that point they are not yet a
// member of the target org and the page has no authenticated way to look
// up "whose invite is this and which org does it name" the way the
// backend's own `resolve_target_org` can. A random DB-backed token gives
// that page nothing to read at all without ITS OWN unauthenticated backend
// round trip (a second capability-token-resolution route, with its own
// enumeration-resistance obligations) just to show "Join Acme?" instead of
// "Join this organization?". A signed JWT's PAYLOAD segment is already
// plain base64url JSON — decodable, un-authenticated, by anyone holding the
// token, WITHOUT verifying the signature — which is exactly the shape a
// pre-auth landing page needs for a purely cosmetic greeting: the page
// reads `org_name` for DISPLAY ONLY; the signature is never checked
// client-side, so a tampered `org_name` in a hand-edited payload segment
// changes nothing about what the SERVER later honours — [`verify_join_
// notice_token`] re-derives every fact that matters (`account_id`,
// `user_id`, `email`, `workspace_id`, `channel_id`) from the signed claims
// independently, and a tampered signature simply fails verification
// (`None`), falling back to an ordinary `self_request` exactly like any
// other invalid token (see [`verify_join_notice_token`]'s own doc comment).
//
// **`HS256` via the `jsonwebtoken` crate, the SAME primitive `oauth::state`
// already uses for its own signed, short-lived, server-only-mintable token**
// (`backend/src/oauth/state.rs` — CSRF-binding an OAuth `state` parameter to
// an account id). `[JoinNoticeClaims]` follows that module's exact shape:
// pure `mint`/`verify` functions taking the secret explicitly (testable
// without env mutation), a `..._secret()` wrapper reading the dedicated env
// var. **A NEW, dedicated `JOIN_NOTICE_SECRET`** (`.env.example`), not a
// reuse of `OAUTH_STATE_SECRET` — this codebase's own established
// convention is one secret per signed-token PURPOSE (`OAUTH_STATE_SECRET`,
// `CLERK_WEBHOOK_SECRET`, `STRIPE_WEBHOOK_SECRET` are none of them shared),
// so rotating one never has to reason about an unrelated feature, and a
// payload-shape collision between two features' claims can never be
// replayed across them.
//
// **No `jti`/single-use store, unlike `oauth::state`.** That token binds a
// one-time OAuth consent and must not be replayable. This one is replayable
// by design: a second click re-hits `services::org_join_requests::
// enqueue_join_request`'s own idempotent `AlreadyOpen`/`Suppressed`
// outcomes.
//
// **CORRECTED (CS-447, 2026-09-22).** This paragraph used to end "clicking
// an old email again is not a security concern, only a no-op." That was
// true when every click produced a queue row an admin still had to look at.
// It is FALSE now: auto-add can turn a click into a membership and a spent
// seat with nobody looking, so a replay of an old link is no longer a
// no-op. Three things, not one, now stand between a stale click and a
// silent add — `AUTO_ADD_MAX_CLASSIFICATION_AGE_SECS` (48h from `iat`,
// far shorter than the link's own 30 days), D10's durable rejection
// suppression, and D12's tombstone — and all three are gates on AUTO-ADD
// only. The click itself still works for 30 days; past 48h it routes to
// the admin queue, which is exactly what it did before auto-add existed.
//
// **30-day expiry** (`JOIN_NOTICE_TOKEN_TTL_SECS`) — this story's own brief
// ("short-lived (30 days)"). An expired token fails [`verify_join_notice_
// token`] identically to a tampered one (`None`), falling back to
// `self_request` — never a distinguishable error (`routes::org_join_
// requests::create`'s own doc comment: "never disclose token validity to
// the caller").
//
// **No new migration, still.** `org_join_requests.source`'s `CHECK` already
// lists `'matched_not_member'` (migration `0238`), and `provider` on THIS
// table has no `CHECK` at all (`0227`) — neither of those facts depended on
// the token's own format, so this correction costs no schema change either.

/// The provider value this nudge claims/renews under — see the section doc
/// above.
pub(crate) const JOIN_THIS_ORG_PROVIDER: &str = "slack_join";

/// [`claim_notice_slot`]'s join-this-org twin — a SIBLING function, not a
/// parameterised `claim_notice_slot`, mirroring [`claim_member_connect_
/// notice_slot`]'s own identical choice for the identical reason (a literal
/// provider string baked into the SQL text, not a bind parameter — see that
/// function's own doc comment). Unlike the member-connect claim,
/// `workspace_id`/`channel_id` ARE genuinely known and bound here: this
/// audience IS discovered by walking a Slack channel's roster
/// (`connectors::slack_unmatched_email::discover_unmatched_recipients`), so
/// the container is real, never `NULL` filler.
///
/// Same claim-as-you-report atomicity, same [`COOLDOWN`], same [`NoticeTokens`]
/// return contract, same caller obligation (a control-plane transaction/
/// connection — no tenant-tree twin) as [`claim_notice_slot`] — see that
/// function's own doc comment for the full reasoning, unchanged here.
///
/// **`NoticeTokens::prefill_token` is minted and stored here (schema-shape
/// symmetry with every other row this table holds — same reasoning
/// [`claim_member_connect_notice_slot`]'s own doc comment gives for its own
/// unused prefill token) but the caller does NOT use it to build this
/// audience's click link** — the click token is a freshly minted, signed
/// JWT ([`mint_join_notice_token`]), independent of this row. Only
/// `unsubscribe_token` is actually read back by this audience's caller.
pub(crate) async fn claim_join_notice_slot(
    executor: impl sqlx::PgExecutor<'_>,
    account_id: Uuid,
    provider: &str,
    workspace_id: &str,
    channel_id: &str,
    email: &str,
) -> Result<Option<NoticeTokens>, sqlx::Error> {
    let candidate_unsub_token = mint_unsubscribe_token();
    let candidate_prefill_token = mint_prefill_token();
    let claimed: Option<(Uuid, String, String)> = sqlx::query_as(&format!(
        "INSERT INTO slack_unmatched_email_notices \
             (account_id, provider, email, workspace_id, channel_id, last_notified_at, \
              unsubscribe_token, signup_prefill_token) \
         VALUES ($1, $7, $2, $3, $4, now(), $5, $6) \
         ON CONFLICT (provider, email) DO UPDATE \
             SET account_id = EXCLUDED.account_id, \
                 last_notified_at = now(), \
                 workspace_id = EXCLUDED.workspace_id, \
                 channel_id = EXCLUDED.channel_id, \
                 unsubscribe_token = COALESCE( \
                     slack_unmatched_email_notices.unsubscribe_token, EXCLUDED.unsubscribe_token), \
                 signup_prefill_token = COALESCE( \
                     slack_unmatched_email_notices.signup_prefill_token, EXCLUDED.signup_prefill_token) \
             WHERE slack_unmatched_email_notices.last_notified_at < now() - INTERVAL '{COOLDOWN}' \
               AND slack_unmatched_email_notices.suppressed_at IS NULL \
         RETURNING id, unsubscribe_token, signup_prefill_token"
    ))
    .bind(account_id)
    .bind(email)
    .bind(workspace_id)
    .bind(channel_id)
    .bind(&candidate_unsub_token)
    .bind(&candidate_prefill_token)
    .bind(provider)
    .fetch_optional(executor)
    .await?;

    Ok(claimed.map(|(_, unsubscribe_token, prefill_token)| NoticeTokens {
        unsubscribe_token,
        prefill_token,
    }))
}

/// Lifetime of a signed join-notice token — this story's own brief
/// ("short-lived (30 days)"). Long enough that a real invite email, read a
/// couple of weeks late, still works; short enough to bound how long a
/// leaked link (a forwarded email, a compromised inbox) stays live. No
/// single-use/`jti` tracking — see the section doc above for why this token
/// is safe to click more than once.
pub(crate) const JOIN_NOTICE_TOKEN_TTL_SECS: i64 = 60 * 60 * 24 * 30;

/// How recent the token's `cls` classification must be for **auto-add** to
/// act on it. Deliberately a DIFFERENT half-life from
/// [`JOIN_NOTICE_TOKEN_TTL_SECS`] above, and the two must not be collapsed
/// back into one constant.
///
/// They answer different questions. The 30 days is about the LINK: "a real
/// invite email, read a couple of weeks late, still works" — a statement
/// about human email habits. This 48 hours is about the CLASSIFICATION the
/// link carries: "is a full member of that Slack workspace" is a fact with
/// its own decay, and CS-447 made it load-bearing — rung 2 of D16's ladder
/// spends a seat on it without an admin looking. Somebody deactivated,
/// downgraded to a guest, or offboarded three weeks after the roster walk
/// is not who that claim says they are, and nothing re-reads the roster
/// between the walk and the click.
///
/// **Only auto-add is gated on this.** An older-than-48h token keeps
/// working exactly as it does today — it routes to the admin approval
/// queue, which is the pre-auto-add behaviour, so nothing regresses for the
/// person clicking. Measured from `iat` (when the classification was made),
/// never from `exp` (when the link dies); `exp` moves with the LINK's
/// half-life and would silently re-couple the two.
pub(crate) const AUTO_ADD_MAX_CLASSIFICATION_AGE_SECS: i64 = 60 * 60 * 48;

/// Claims embedded in a signed join-notice token — see the section doc
/// above ("The click token: a signed JWT"). Short field names, same
/// convention `oauth::state::StateClaims` uses, for a smaller token.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct JoinNoticeClaims {
    /// Subject — this person's EXISTING CoralSwarm `users.id`. The
    /// authoritative identity check: `routes::org_join_requests::create`
    /// requires this to equal the AUTHENTICATED caller's own `user_id`, so
    /// a forwarded link only ever produces an honest `self_request` for
    /// whoever actually clicks it, never a request filed as someone else.
    sub: String,
    /// The target org's `accounts.id`.
    acc: String,
    /// The target org's Clerk id — redundant with `acc` (both name the same
    /// org) but kept so the route's own `org` path param can be cross-
    /// checked against the token without an extra DB round trip.
    org: String,
    /// The address this invite was sent to — normalised (lowercase+trim),
    /// carried into `org_join_requests.evidence.email` on a successful
    /// click. Not itself part of the authorization decision (`sub`/`acc`
    /// already fully authorize it) — kept for the same audit reason
    /// `evidence` exists on every OTHER source this table records.
    email: String,
    /// The Slack workspace/channel this invite's own roster walk
    /// discovered the recipient through — `org_join_requests.evidence`'s
    /// `workspace_id`/`channel_id`. Absent, not `""`, when genuinely
    /// unknown (defensive; every real mint site has both).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    wsp: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    chn: Option<String>,
    /// The org's Clerk DISPLAY NAME at mint time — DISPLAY ONLY, for
    /// CS-439's `/join/[org]` landing page to read from the token's own
    /// (unverified, pre-signature-check) payload segment before the
    /// requester is authenticated against this org at all. Never read by
    /// [`verify_join_notice_token`]'s own caller for any authorization
    /// decision — see the section doc's "CORRECTED" paragraph for the full
    /// reasoning. A tampered value here changes nothing server-side.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    org_name: Option<String>,
    /// **CS-447 (item 9, D16 rung 2).** How the roster walk that produced
    /// this invite CLASSIFIED the recipient in the org's Slack workspace —
    /// `"full_member"`, `"guest"`, `"slack_connect"`, or absent.
    ///
    /// This is the only way D16's rung-2 workspace half reaches
    /// `routes::org_join_requests::create`, which has no live Slack
    /// connection of its own (see `workspace_signal_from_token`'s doc
    /// comment for why firing one from a requester-facing POST is the wrong
    /// shape). The classification is made ONCE, on a connected member's own
    /// token, by `connectors::slack_unmatched_email::discover_unmatched_
    /// recipients`; this claim carries it rather than re-deriving it.
    ///
    /// **It must be carried, not inferred from the token's existence.**
    /// That audience deliberately includes a `Guest`/`SlackConnect`
    /// participant whose address is on the org's anchor domain (D2/D21), so
    /// "we emailed them a join link" is NOT "they are a full member" — and
    /// D16 is explicit that only the second is worth anything. Folding the
    /// two together would auto-add a client's guest account on the strength
    /// of an email we sent them.
    ///
    /// Signed like every other claim, so it cannot be edited by the holder;
    /// absent on a token minted before CS-447, which reads as
    /// `WorkspaceSignal::Unknown` and never admits.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    cls: Option<String>,
    /// Issued-at (unix seconds) — becomes `org_join_requests.evidence.
    /// invited_at` on a successful click.
    iat: i64,
    /// Expiry (unix seconds), `iat + JOIN_NOTICE_TOKEN_TTL_SECS`. Validated
    /// by `jsonwebtoken`.
    exp: i64,
}

/// Minimum accepted length of `JOIN_NOTICE_SECRET`, in BYTES.
///
/// HS256's security rests entirely on this key: the token is a bearer
/// credential that CS-447 turned into an admission decision (see
/// [`join_notice_secret`]), and it travels through third-party mail
/// infrastructure, so an attacker holding one captured token can brute-force
/// a short key OFFLINE — no rate limit, no logs, no alert. 32 bytes is the
/// HMAC-SHA256 block-size floor RFC 2104 §3 recommends ("keys longer than B
/// bytes are first hashed"; keys shorter than the output length are the case
/// it warns about), and is what `jsonwebtoken` itself documents for HS256.
///
/// This is a length floor, not an entropy measure — 32 bytes of ASCII prose
/// passes it. It is the cheap half of the check; the expensive half is
/// generating the value with `openssl rand -base64 48`, which is what
/// `.env.example` says to do.
pub(crate) const JOIN_NOTICE_SECRET_MIN_BYTES: usize = 32;

/// Read the join-notice-token signing secret from the environment. `None`
/// when `JOIN_NOTICE_SECRET` is unset, empty, or SHORTER THAN
/// [`JOIN_NOTICE_SECRET_MIN_BYTES`] — every caller treats all three
/// identically, as "cannot mint/verify this audience's links right now",
/// never a guess (the SEND side skips the whole matched-not-member pass
/// rather than emailing a dead link; the CLICK side falls back to
/// `self_request`, identical to any other invalid-token outcome).
///
/// **A too-short secret is refused rather than used.** Refusing degrades one
/// feature to its pre-CS-440 behaviour, which is a fully supported state the
/// send and click sides both already handle; accepting would sign an
/// admission decision with a key that can be recovered offline from a single
/// emailed token. The warning names the variable and reports LENGTHS only —
/// never the value, which would defeat the whole point by writing the secret
/// into the log stream.
pub(crate) fn join_notice_secret() -> Option<String> {
    usable_join_notice_secret(std::env::var("JOIN_NOTICE_SECRET").ok())
}

/// [`join_notice_secret`]'s rule, with the environment read out of it.
///
/// Split this way for the reason the section doc above already gives for
/// `mint`/`verify` ("pure functions taking the secret explicitly, testable
/// without env mutation, a `..._secret()` wrapper reading the dedicated env
/// var") — and here it matters more than convenience: the lib test binary
/// runs in parallel, and a test that set `JOIN_NOTICE_SECRET` to assert the
/// floor would race every other test in the process that reads it.
pub(crate) fn usable_join_notice_secret(raw: Option<String>) -> Option<String> {
    let raw = raw?;
    if raw.is_empty() {
        return None;
    }
    if raw.len() < JOIN_NOTICE_SECRET_MIN_BYTES {
        tracing::warn!(
            found_bytes = raw.len(),
            required_bytes = JOIN_NOTICE_SECRET_MIN_BYTES,
            "JOIN_NOTICE_SECRET is set but too short to sign with — treating it as UNSET. \
             Join-notice links will not be minted or accepted until it is replaced with at \
             least 32 bytes (`openssl rand -base64 48`)."
        );
        return None;
    }
    Some(raw)
}

/// What a valid join-this-org token verifies to — everything `routes::
/// org_join_requests::create` needs to decide whether to honour it.
pub(crate) struct JoinNoticeContext {
    pub(crate) account_id: Uuid,
    pub(crate) user_id: Uuid,
    pub(crate) email: String,
    pub(crate) workspace_id: Option<String>,
    pub(crate) channel_id: Option<String>,
    pub(crate) invited_at: chrono::DateTime<chrono::Utc>,
    /// **CS-447.** The roster walk's own workspace classification, parsed
    /// from the signed `cls` claim — D16 rung 2's workspace half. `None`
    /// for an absent or unrecognised claim (a token minted before CS-447,
    /// or a member the walk could not classify), which
    /// `assertion_ladder` reads as `WorkspaceSignal::Unknown` and which
    /// never admits.
    pub(crate) membership_class: Option<crate::connectors::slack::MembershipClass>,
}

/// Mint a signed join-notice token — pure (no DB, no I/O), same "secret
/// taken explicitly" shape as `oauth::state::sign_state` (testable without
/// env mutation). `workers::slack_unmatched_email::process_matched_not_
/// members` is the one real caller, resolving [`join_notice_secret`] once
/// per job and passing it in.
#[allow(clippy::too_many_arguments)]
pub(crate) fn mint_join_notice_token(
    secret: &str,
    account_id: Uuid,
    user_id: Uuid,
    clerk_org_id: &str,
    email: &str,
    workspace_id: Option<&str>,
    channel_id: Option<&str>,
    org_name: Option<&str>,
    membership_class: crate::connectors::slack::MembershipClass,
) -> Result<String, String> {
    mint_join_notice_token_at(
        secret,
        account_id,
        user_id,
        clerk_org_id,
        email,
        workspace_id,
        channel_id,
        org_name,
        membership_class,
        chrono::Utc::now().timestamp(),
    )
}

/// [`mint_join_notice_token`] with `iat` supplied rather than read from the
/// clock.
///
/// Exists so the 48-hour auto-add freshness bound
/// ([`AUTO_ADD_MAX_CLASSIFICATION_AGE_SECS`]) can be tested for what it
/// actually is — a token that is STILL VALID as a link (its 30-day `exp`
/// has not passed) but whose classification is too old to auto-add on. That
/// state is unreachable by minting at `now` and unreachable by waiting, so
/// without this the one gate standing between a three-week-old roster walk
/// and a silently spent seat would have no test at all.
///
/// Production mints at `now` and only ever through
/// [`mint_join_notice_token`]; nothing here lets a caller forge a claim it
/// could not already forge by holding the secret.
#[allow(clippy::too_many_arguments)]
pub(crate) fn mint_join_notice_token_at(
    secret: &str,
    account_id: Uuid,
    user_id: Uuid,
    clerk_org_id: &str,
    email: &str,
    workspace_id: Option<&str>,
    channel_id: Option<&str>,
    org_name: Option<&str>,
    membership_class: crate::connectors::slack::MembershipClass,
    issued_at: i64,
) -> Result<String, String> {
    let now = issued_at;
    let claims = JoinNoticeClaims {
        sub: user_id.to_string(),
        acc: account_id.to_string(),
        org: clerk_org_id.to_string(),
        email: email.to_string(),
        wsp: workspace_id.map(str::to_string),
        chn: channel_id.map(str::to_string),
        org_name: org_name.map(str::to_string),
        // `Unavailable` mints NO claim — see `MembershipClass::label`.
        cls: membership_class.label().map(str::to_string),
        iat: now,
        exp: now + JOIN_NOTICE_TOKEN_TTL_SECS,
    };
    jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
        &claims,
        &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
    )
    .map_err(|e| format!("mint_join_notice_token: {e}"))
}

/// Verify a signed join-notice token and return its claims. `None` for ANY
/// failure — bad signature, tampering, expiry, a non-UUID `sub`/`acc` —
/// deliberately without distinguishing the cause (the identical "ask,
/// don't guess, and don't build an oracle" discipline `oauth::state::
/// verify_state`'s own doc comment states). Pure: no DB access, no async —
/// `routes::org_join_requests::create` calls this synchronously, unlike the
/// FIRST version of this function (see the section doc's "CORRECTED"
/// paragraph), which needed two round trips just to resolve a bare token.
pub(crate) fn verify_join_notice_token(secret: &str, token: &str) -> Option<JoinNoticeContext> {
    let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
    validation.validate_exp = true;
    validation.set_required_spec_claims(&["exp"]);

    let data = jsonwebtoken::decode::<JoinNoticeClaims>(
        token,
        &jsonwebtoken::DecodingKey::from_secret(secret.as_bytes()),
        &validation,
    )
    .ok()?;
    let c = data.claims;
    let account_id = Uuid::parse_str(&c.acc).ok()?;
    let user_id = Uuid::parse_str(&c.sub).ok()?;
    let invited_at = chrono::DateTime::from_timestamp(c.iat, 0).unwrap_or_else(chrono::Utc::now);
    // `org` (the Clerk id) is carried on the token but not re-validated
    // here — `routes::org_join_requests::create` already resolved its OWN
    // `account_id` from the request body's `org` field before ever calling
    // this function, so comparing `account_id == that resolved id` (which
    // it does) is the authoritative check; a mismatched `c.org` with a
    // MATCHING `c.acc` cannot happen for a token this module itself minted
    // (both are written from the same `account_id`/`clerk_org_id` pair at
    // mint time), so there is nothing an independent `org` check would
    // catch that the `account_id` comparison does not already catch.
    let _ = c.org;
    // Unrecognised or absent => `None`, never a guess (CS-447).
    let membership_class =
        c.cls.as_deref().and_then(crate::connectors::slack::MembershipClass::from_label);
    Some(JoinNoticeContext {
        account_id,
        user_id,
        email: c.email,
        membership_class,
        workspace_id: c.wsp,
        channel_id: c.chn,
        invited_at,
    })
}

#[cfg(test)]
mod join_notice_token_tests {
    //! Pure, DB-and-network-free — same reasoning as `oauth::state::tests`
    //! (the module this shape was copied from): [`mint_join_notice_token`]/
    //! [`verify_join_notice_token`] take their secret explicitly, so no env
    //! mutation or lock is needed here.

    use super::{mint_join_notice_token, verify_join_notice_token};
    use uuid::Uuid;

    const SECRET: &str = "test-join-notice-secret-0123456789";

    /// **M2 (security review, 2026-09-22) — a too-short secret is refused,
    /// not used.**
    ///
    /// HS256's whole security is this key, the token is a bearer credential
    /// that CS-447 turned into an admission decision, and it travels
    /// through third-party mail infrastructure — so a short key is
    /// brute-forceable OFFLINE from one captured token, with no rate limit
    /// and nothing to alert on.
    ///
    /// The assertion that matters is the LAST one: too-short must be
    /// indistinguishable from unset, because "unset" is the state both the
    /// send side (skip the pass) and the click side (fall back to
    /// `self_request`) already handle correctly. Anything else would be a
    /// new, untested failure mode.
    #[test]
    fn a_secret_shorter_than_the_floor_is_treated_exactly_like_unset() {
        use super::{usable_join_notice_secret, JOIN_NOTICE_SECRET_MIN_BYTES};

        assert_eq!(JOIN_NOTICE_SECRET_MIN_BYTES, 32);

        let floor = "x".repeat(JOIN_NOTICE_SECRET_MIN_BYTES);
        assert_eq!(
            usable_join_notice_secret(Some(floor.clone())),
            Some(floor),
            "exactly 32 bytes is accepted — the floor is inclusive"
        );
        let long = "y".repeat(64);
        assert_eq!(usable_join_notice_secret(Some(long.clone())), Some(long));
        assert_eq!(usable_join_notice_secret(Some(SECRET.to_string())), Some(SECRET.to_string()));

        for short in [
            String::new(),
            "x".to_string(),
            "hunter2".to_string(),
            "x".repeat(JOIN_NOTICE_SECRET_MIN_BYTES - 1),
        ] {
            assert_eq!(
                usable_join_notice_secret(Some(short.clone())),
                None,
                "a {}-byte secret must be refused",
                short.len()
            );
        }
        assert_eq!(usable_join_notice_secret(None), None);
        assert_eq!(
            usable_join_notice_secret(Some("x".repeat(JOIN_NOTICE_SECRET_MIN_BYTES - 1))),
            usable_join_notice_secret(None),
            "too-short and unset must be the SAME answer — callers must not need to tell them apart"
        );
    }

    #[test]
    fn round_trips_every_claim() {
        let account_id = Uuid::new_v4();
        let user_id = Uuid::new_v4();
        let token = mint_join_notice_token(
            SECRET,
            account_id,
            user_id,
            "org_acme",
            "dana@example.com",
            Some("T1"),
            Some("C1"),
            Some("Acme"),
            // CS-447: no classification to carry — this test is about the
            // token's OWN validity, not D16 rung 2. `Unavailable` mints
            // NO `cls` claim, so the token shape here is byte-identical
            // to what it was before CS-447.
            crate::connectors::slack::MembershipClass::Unavailable,
        )
        .unwrap();

        let ctx = verify_join_notice_token(SECRET, &token).expect("a freshly minted token must verify");
        assert_eq!(ctx.account_id, account_id);
        assert_eq!(ctx.user_id, user_id);
        assert_eq!(ctx.email, "dana@example.com");
        assert_eq!(ctx.workspace_id.as_deref(), Some("T1"));
        assert_eq!(ctx.channel_id.as_deref(), Some("C1"));
    }

    /// FAILS WITHOUT THE FIX: the payload segment must be plain, readable
    /// base64url JSON (CS-439's own need — see the section doc's
    /// "CORRECTED" paragraph) — `org_name` must be recoverable WITHOUT
    /// verifying the signature, by decoding the middle `.`-segment alone.
    #[test]
    fn the_payload_segment_is_plain_base64url_json_carrying_org_name() {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};

        let token = mint_join_notice_token(
            SECRET,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "org_acme",
            "dana@example.com",
            None,
            None,
            Some("Acme & Co"),
            // CS-447: no classification to carry — this test is about the
            // token's OWN validity, not D16 rung 2. `Unavailable` mints
            // NO `cls` claim, so the token shape here is byte-identical
            // to what it was before CS-447.
            crate::connectors::slack::MembershipClass::Unavailable,
        )
        .unwrap();

        let payload_segment = token.split('.').nth(1).expect("a JWT has three dot-separated segments");
        let decoded = URL_SAFE_NO_PAD.decode(payload_segment).expect("payload must be valid base64url");
        let json: serde_json::Value = serde_json::from_slice(&decoded).expect("payload must be valid JSON");
        assert_eq!(
            json["org_name"], "Acme & Co",
            "org_name must be readable from the payload without verifying the signature"
        );
    }

    /// FAILS WITHOUT THE FIX: a tampered signature must never verify — same
    /// "ask, don't guess" contract `oauth::state::verify_state` states for
    /// itself.
    #[test]
    fn a_tampered_token_does_not_verify() {
        let token = mint_join_notice_token(
            SECRET,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "org_acme",
            "dana@example.com",
            Some("T1"),
            Some("C1"),
            None,
            // CS-447: no classification to carry — this test is about the
            // token's OWN validity, not D16 rung 2. `Unavailable` mints
            // NO `cls` claim, so the token shape here is byte-identical
            // to what it was before CS-447.
            crate::connectors::slack::MembershipClass::Unavailable,
        )
        .unwrap();
        let mut tampered = token.clone();
        tampered.push('x');
        assert!(verify_join_notice_token(SECRET, &tampered).is_none());
    }

    #[test]
    fn a_token_signed_with_a_different_secret_does_not_verify() {
        let token = mint_join_notice_token(
            SECRET,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "org_acme",
            "dana@example.com",
            None,
            None,
            None,
            // CS-447: no classification to carry — this test is about the
            // token's OWN validity, not D16 rung 2. `Unavailable` mints
            // NO `cls` claim, so the token shape here is byte-identical
            // to what it was before CS-447.
            crate::connectors::slack::MembershipClass::Unavailable,
        )
        .unwrap();
        assert!(verify_join_notice_token("a-completely-different-secret", &token).is_none());
    }

    /// FAILS WITHOUT THE FIX: this story's own 30-day TTL must actually be
    /// enforced — a token minted with `iat`/`exp` already in the past must
    /// not verify.
    #[test]
    fn an_expired_token_does_not_verify() {
        // Mint via the real function, then hand-craft an already-expired
        // token with the SAME claims shape rather than mutating `exp` after
        // the fact (there is no public setter, by design — expiry is always
        // `iat + JOIN_NOTICE_TOKEN_TTL_SECS`, never caller-chosen).
        #[derive(serde::Serialize)]
        struct ExpiredClaims<'a> {
            sub: String,
            acc: String,
            org: &'a str,
            email: &'a str,
            iat: i64,
            exp: i64,
        }
        let claims = ExpiredClaims {
            sub: Uuid::new_v4().to_string(),
            acc: Uuid::new_v4().to_string(),
            org: "org_acme",
            email: "dana@example.com",
            iat: 1,
            exp: 2, // long expired
        };
        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret(SECRET.as_bytes()),
        )
        .unwrap();
        assert!(verify_join_notice_token(SECRET, &token).is_none());
    }

    #[test]
    fn garbage_input_does_not_verify() {
        assert!(verify_join_notice_token(SECRET, "not-a-jwt-at-all").is_none());
    }
}

/// This email's dynamic copy — `org_label` is already sanitised (see
/// `workers::slack_unmatched_email::resolve_org_display_name`'s own doc
/// comment) but NOT yet HTML-escaped; callers pass the ESCAPED label so the
/// heading's `<em>` wrapping never re-encodes it. D6: one clear, one-line
/// CTA — the button text itself IS that line ("Join {org} on coralswarm"),
/// not a separate sentence elsewhere in the body.
fn join_org_copy(org_label_escaped: &str) -> (String, String) {
    let em = |word: &str| format!("<em style=\"font-style:italic;color:#D14A38\">{word}</em>");
    (
        format!("Join {org_label_escaped} on coralswarm"),
        format!("Join {}", em(org_label_escaped)),
    )
}

/// Render the join-this-org email body. Same structural shell as
/// [`render_nudge_email_html`]/[`render_connect_nudge_email_html`], its OWN
/// copy end to end (this module's own "own copy matrix" convention).
///
/// **Copy honesty (house rule, this story's brief).** States only what is
/// provable: CoralSwarm is already active in a Slack channel this recipient
/// shares with the org (true — the bot is a member of it), and that they
/// already have a CoralSwarm account elsewhere (true — that is the whole
/// premise of this audience). It never claims "your teammates are already
/// here" (unprovable — this walk only proves ONE shared channel, not that
/// anyone else there is a colleague) and never mentions a pool, a prior
/// rejection, or whether the click will succeed — the recipient must never
/// learn the pool state or a prior rejection (this story's brief,
/// verbatim). Neither "slot" (the billing place) nor "seat" (team-ocean
/// access) appears anywhere: this email is about ORG MEMBERSHIP, neither of
/// those two concepts.
fn render_join_org_email_html(
    to_email: &str,
    org_label: &str,
    join_url: &str,
    web_base: &str,
    unsubscribe_url: &str,
) -> String {
    let email_esc = escape_html(to_email);
    let org_esc = escape_html(org_label);
    let url_esc = escape_html(join_url);
    let host_esc = escape_html(host_display(web_base));
    let unsubscribe_esc = escape_html(unsubscribe_url);
    let (_, heading) = join_org_copy(&org_esc);

    format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width\"></head>\
<body style=\"margin:0;padding:0;background:#F2EBE0\">\
<div style=\"display:none;max-height:0;overflow:hidden;mso-hide:all\">CoralSwarm is already active in a Slack channel you're both in — you already have an account, so joining takes one click.</div>\
<table role=\"presentation\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" bgcolor=\"#F2EBE0\"><tr><td align=\"center\" style=\"padding:40px 16px\">\
<table role=\"presentation\" width=\"560\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" style=\"width:560px;max-width:100%\">\
<tr><td bgcolor=\"#FBF7F2\" style=\"border:1px solid #D9CCB8;border-radius:12px;padding:34px 36px;font-family:{FONT_SANS};color:#1A1410\">\
<div style=\"margin-bottom:26px;font-weight:600;font-size:19px;letter-spacing:-0.045em;color:#1A1410;line-height:1\">coralswarm</div>\
<div style=\"font-family:{FONT_MONO};font-size:10px;text-transform:uppercase;letter-spacing:0.14em;color:#8A7B6E;margin-bottom:12px\">you already use coralswarm</div>\
<h1 style=\"margin:0 0 14px 0;font-family:{FONT_SERIF};font-size:30px;font-weight:400;line-height:1.2;color:#1A1410\">{heading}</h1>\
<p style=\"margin:0 0 16px 0;font-size:14px;line-height:1.6;color:#4A3F36\">CoralSwarm is already saving a Slack channel you're both in at {org_esc}. You already have your own CoralSwarm account (<span style=\"font-family:{FONT_MONO};font-size:13px;color:#1A1410\">{email_esc}</span>) — this is not a new signup, it's a request to join {org_esc} with the account you already have.</p>\
<table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" style=\"margin-bottom:12px\"><tr>\
<td bgcolor=\"#F77764\" style=\"border-radius:8px\">\
<a href=\"{url_esc}\" style=\"display:inline-block;padding:11px 22px;font-family:{FONT_SANS};font-size:15px;font-weight:500;color:#FFFFFF;text-decoration:none\">Join {org_esc} on coralswarm</a>\
</td></tr></table>\
<p style=\"margin:0 0 26px 0;font-family:{FONT_MONO};font-size:10.5px;line-height:1.6;color:#8A7B6E\">opens at {host_esc} · you'll already be signed in</p>\
<table role=\"presentation\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\"><tr>\
<td style=\"border-top:1px solid #D9CCB8;padding-top:16px\">\
<p style=\"margin:0 0 10px 0;font-family:{FONT_MONO};font-size:9.5px;line-height:1.7;color:#8A7B6E\">An admin at {org_esc} reviews every request before you get access — nothing here creates an account or captures anything on your behalf.</p>\
<p style=\"margin:0;font-family:{FONT_MONO};font-size:9.5px;line-height:1.7\"><a href=\"{unsubscribe_esc}\" style=\"color:#8A7B6E;text-decoration:underline\">Turn off this reminder</a></p>\
</td></tr></table>\
</td></tr>\
<tr><td align=\"center\" style=\"padding-top:14px\">\
<p style=\"margin:0;font-family:{FONT_MONO};font-size:9.5px;color:#8A7B6E\">coralswarm · your team's living memory</p>\
</td></tr></table>\
</td></tr></table></body></html>"
    )
}

/// Send one join-this-org email via Resend — same best-effort contract as
/// [`send_nudge_email`]/[`send_connect_nudge_email`] (any failure returns
/// `false`, never panics or propagates) and the SAME "send before commit"
/// caller obligation as [`send_connect_nudge_email`] (see that function's
/// own doc comment): the caller must hold the claim's transaction open
/// across this call and only commit on `true` — a failed send here must
/// never burn this recipient's 7-day cooldown window.
///
/// `join_url` is built by the caller (`{web_base}/join/{clerk_org_id}?t=
/// {join_token}`) rather than assembled in here, unlike the two sibling
/// send functions' own in-function URL building: the PATH segment needs the
/// target org's `clerk_org_id`, a fact this function has no other reason to
/// take a parameter for, and building it at the one call site keeps this
/// function's own parameter list matching its siblings' shape
/// (`to_email`, copy inputs, `web_base`, `unsubscribe_token`).
pub(crate) async fn send_join_org_email(
    state: &AppState,
    to_email: &str,
    org_label: &str,
    join_url: &str,
    web_base: &str,
    unsubscribe_token: &str,
) -> bool {
    let Some(resend_key) = state.resend_api_key.clone() else {
        return false;
    };
    // CodeRabbit round 1 (#1026): the subject is a plain-text header value,
    // never rendered as HTML — build it from the RAW label. The sanitizer
    // `workers::slack_unmatched_email::resolve_org_display_name` applies
    // still allows `&` (the same allowlist `org_invites::safe_label` uses),
    // so an earlier version of this line HTML-escaped it into a literal
    // `&amp;` that then went out verbatim in the Subject header — this was
    // wrong, not merely redundant.
    let (subject, _) = join_org_copy(org_label);
    let unsubscribe_url = format!(
        "{}/v1/email/unsubscribe?t={unsubscribe_token}",
        state.api_base_url()
    );
    let payload = serde_json::json!({
        "from": "coralswarm <person@example.com>",
        "to": [to_email],
        "subject": subject,
        "html": render_join_org_email_html(to_email, org_label, join_url, web_base, &unsubscribe_url),
        "headers": {
            "List-Unsubscribe": format!("<{unsubscribe_url}>"),
            "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
        },
    });
    let url = format!("{}/emails", resend_base_url());
    let send = state.http_client.post(&url).bearer_auth(&resend_key).json(&payload).send();
    match tokio::time::timeout(std::time::Duration::from_secs(10), send).await {
        Ok(Ok(resp)) if resp.status().is_success() => true,
        Ok(Ok(resp)) => {
            tracing::warn!(
                status = %resp.status(), email_domain = email_domain(to_email),
                "unmatched_recipient_email: Resend returned non-success for join-this-org email"
            );
            false
        }
        Ok(Err(e)) => {
            tracing::warn!(
                error = %e, email_domain = email_domain(to_email),
                "unmatched_recipient_email: failed to send join-this-org email via Resend"
            );
            false
        }
        Err(_) => {
            tracing::warn!(
                email_domain = email_domain(to_email),
                "unmatched_recipient_email: Resend send timed out for join-this-org email"
            );
            false
        }
    }
}

#[cfg(test)]
mod send_join_org_email_tests {
    //! Network-but-no-database tests for [`send_join_org_email`] — a real
    //! HTTP POST against a local mock Resend server, same harness shape as
    //! `send_nudge_email_tests` above (this module's own copy matrix — a
    //! shared test-state helper across the two would couple two otherwise
    //! independent send functions' tests for no real gain).

    use super::*;
    use std::sync::{Arc as StdArc, Mutex as StdMutex};

    // `super::RESEND_BASE_URL_LOCK`, shared with every other lib test that
    // writes `RESEND_API_BASE_URL`. A lock private to this module lets
    // `send_nudge_email_tests` (and `workers::slack_unmatched_email`) point
    // the var at their own mock while this test is in flight.

    async fn spawn_mock_resend() -> (String, StdArc<StdMutex<Option<serde_json::Value>>>) {
        use axum::{routing::post, Json, Router};

        let captured: StdArc<StdMutex<Option<serde_json::Value>>> = StdArc::new(StdMutex::new(None));
        let captured_for_handler = captured.clone();
        let handler = move |Json(body): Json<serde_json::Value>| {
            let captured = captured_for_handler.clone();
            async move {
                *captured.lock().unwrap() = Some(body);
                Json(serde_json::json!({ "id": "mock" }))
            }
        };
        let app = Router::new().route("/emails", post(handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock resend listener");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        (format!("http://{addr}"), captured)
    }

    fn test_state(resend_api_key: &str) -> AppState {
        let resolver =
            crate::tenancy::secrets::StaticMapResolver::from_pairs(Vec::<(&str, &str)>::new());
        let registry = std::sync::Arc::new(crate::tenancy::registry::TenantRegistry::new(
            std::sync::Arc::new(resolver),
        ));
        let tenant_runtime = crate::tenancy::runtime::TenantRuntime::new_with_web_apex(
            true,
            "api.coralswarm.test",
            "coralswarm.test",
            registry,
        );
        AppState {
            db: None,
            store: None,
            resend_api_key: Some(resend_api_key.to_string()),
            r2: None,
            http_client: reqwest::Client::new(),
            embedder: StdArc::new(crate::StubEmbedder),
            llm: None,
            rate_limiter: StdArc::new(crate::rate_limit::InMemoryRateLimiter::new()),
            admission: Default::default(),
            in_flight: StdArc::new(Default::default()),
            metrics: StdArc::new(crate::telemetry::NoopMetrics),
            dek_cache: crate::dek_cache::DekCache::new(),
            connector_flag_cache: crate::connector_flags::ConnectorFlagCache::new(),
            tenant_runtime: Some(tenant_runtime),
            env: crate::AppEnv::default(),
            task_tracker: tokio_util::task::TaskTracker::new(),
            worker_activity: crate::workers::tenant_harness::TenantActivity::new(),
            mcp_cancellation_token: tokio_util::sync::CancellationToken::new(),
            mcp_task_manager: crate::mcp::server::McpTaskRegistry::new(),
            mcp_ask_in_flight: crate::mcp::server::AskInFlight::new(),
            stripe: None,
        }
    }

    /// FAILS WITHOUT THE FIX (CodeRabbit round 1, #1026, Functional
    /// Correctness): an earlier version HTML-escaped the org label before
    /// building the SUBJECT (a plain-text header, never rendered as HTML),
    /// so `"Acme & Co"` went out as the literal five characters `&amp;`
    /// inside the Subject line instead of a bare `&`. The HTML BODY must
    /// still carry the escaped form.
    #[tokio::test]
    async fn subject_carries_the_raw_org_label_body_carries_the_escaped_one() {
        let _guard = super::RESEND_BASE_URL_LOCK.lock().unwrap();
        let (resend_base, captured) = spawn_mock_resend().await;
        // SAFETY: serialized by `_guard` above, restored before it drops.
        unsafe {
            std::env::set_var("RESEND_API_BASE_URL", &resend_base);
        }

        let state = test_state("re_mock_key");
        let sent = send_join_org_email(
            &state,
            "person@example.com",
            "Acme & Co",
            "https://app.coralswarm.test/join/org_acme?t=tok",
            "https://app.coralswarm.test",
            "unsub-tok",
        )
        .await;

        unsafe {
            std::env::remove_var("RESEND_API_BASE_URL");
        }
        assert!(sent, "the mock server answers 2xx, so this must report success");

        let body = captured.lock().unwrap().take().expect("mock resend must have received a request");
        let subject = body["subject"].as_str().expect("payload must carry a subject field");
        assert_eq!(
            subject, "Join Acme & Co on coralswarm",
            "the Subject header must carry the RAW org label — a literal '&', never '&amp;'"
        );
        assert!(!subject.contains("&amp;"), "the subject must never contain an HTML entity");

        let html = body["html"].as_str().expect("payload must carry an html field");
        assert!(
            html.contains("Acme &amp; Co"),
            "the HTML BODY must still escape the org label — subject and body are independent"
        );
    }
}

#[cfg(test)]
mod copy_tests {
    //! Pure, DB-and-network-free tests — no `ORGT_TEST_BASE_URL` needed,
    //! same reasoning as `connectors::slack::extract_verified_slack_profile`'s
    //! own pure test module: everything here is string-in, string-out.

    use super::{escape_html, host_display, render_nudge_email_html};

    /// FAILS WITHOUT THE FIX: the whole point of this email (this module's
    /// own doc comment, contract I5) is that it must never read as "capture
    /// depends on you" — this pins the literal sentence that says so, so a
    /// future copy edit that drops it fails loudly instead of silently
    /// reintroducing the exact ambiguity I5 forbids.
    #[test]
    fn body_states_capture_is_not_gated_on_signing_in() {
        let html = render_nudge_email_html(
            "person@example.com",
            "https://app.example.com/join",
            "https://app.example.com",
            "https://app.example.com/v1/email/unsubscribe?t=tok",
        );
        assert!(
            html.contains("captured whether or not you create an account"),
            "the email must plainly state capture does not depend on signing in (I5)"
        );
    }

    /// FAILS WITHOUT THE FIX: the whole point of this PR is that the header
    /// alone (invisible in a rendered message) does not satisfy "clear and
    /// conspicuous" — there must be a real, clickable link in the body.
    #[test]
    fn body_contains_a_visible_unsubscribe_link() {
        let html = render_nudge_email_html(
            "person@example.com",
            "https://app.example.com/join",
            "https://app.example.com",
            "https://app.example.com/v1/email/unsubscribe?t=abc123",
        );
        assert!(
            html.contains("href=\"https://app.example.com/v1/email/unsubscribe?t=abc123\""),
            "the unsubscribe URL must appear as a real, clickable href in the body"
        );
        assert!(
            html.to_lowercase().contains("unsubscribe"),
            "the link text itself must say 'unsubscribe' (clear and conspicuous)"
        );
    }

    #[test]
    fn dynamic_values_are_escaped() {
        let html = render_nudge_email_html(
            "a<b>@example.com",
            "https://app.example.com/join?email=a&x=1",
            "https://app.example.com",
            "https://app.example.com/v1/email/unsubscribe?t=a&b",
        );
        assert!(!html.contains("a<b>@example.com"), "raw email must not appear unescaped");
        assert!(html.contains("a&lt;b&gt;@example.com"), "email must be HTML-escaped");
        assert!(html.contains("&amp;x=1"), "join_url's query separators must be HTML-escaped");
        assert!(
            html.contains("t=a&amp;b"),
            "the unsubscribe URL's query separators must be HTML-escaped too, same as join_url"
        );
    }

    #[test]
    fn host_display_strips_scheme_and_path() {
        assert_eq!(host_display("https://acme.coralswarm.example/anything"), "acme.coralswarm.example");
        assert_eq!(host_display("http://localhost:3000"), "localhost:3000");
    }

    #[test]
    fn escape_html_covers_all_five_entities() {
        assert_eq!(escape_html("<>&\"'"), "&lt;&gt;&amp;&quot;&#39;");
    }
}

#[cfg(test)]
mod connect_nudge_copy_tests {
    //! Pure, DB-and-network-free — same reasoning as `copy_tests` above.

    use super::{escape_html, render_connect_nudge_email_html};

    /// House rule (this story's brief): copy may only claim what the data
    /// proves — "hasn't connected a tool yet", never "has no data"/"empty
    /// ocean". FAILS WITHOUT THE FIX if a future edit reintroduces either
    /// forbidden phrase or drops the honest one.
    #[test]
    fn body_states_the_true_thing_not_the_overreach() {
        let html = render_connect_nudge_email_html(
            "person@example.com",
            "https://acme.coralswarm.example/settings/organization/connectors",
            "https://acme.coralswarm.example",
            "https://api.coralswarm.example/v1/email/unsubscribe?t=tok",
        );
        assert!(
            html.contains("haven't connected a tool yet"),
            "must state the true, narrow claim"
        );
        assert!(!html.to_lowercase().contains("empty ocean"), "must not claim the ocean is empty");
        assert!(!html.to_lowercase().contains("no data"), "must not claim there is no data");
    }

    /// D15: "slot" is the billing place, "seat" is team-ocean access — this
    /// email is neither about billing nor about team-ocean access, so
    /// neither word belongs in it (this story's brief, verbatim).
    #[test]
    fn body_never_says_seat_or_slot() {
        let html = render_connect_nudge_email_html(
            "person@example.com",
            "https://acme.coralswarm.example/settings/organization/connectors",
            "https://acme.coralswarm.example",
            "https://api.coralswarm.example/v1/email/unsubscribe?t=tok",
        );
        let lower = html.to_lowercase();
        assert!(!lower.contains("seat"), "must never say 'seat' (D15)");
        assert!(!lower.contains("slot"), "must never say 'slot' (D15)");
    }

    /// This audience IS already captured-eligible content-wise (D21: nothing
    /// is captured on their behalf until THEY connect) — the sibling nudge's
    /// "captured whether or not you create an account" sentence would be
    /// FALSE here and must never appear.
    #[test]
    fn body_does_not_reuse_the_sibling_nudges_already_captured_claim() {
        let html = render_connect_nudge_email_html(
            "person@example.com",
            "https://acme.coralswarm.example/settings/organization/connectors",
            "https://acme.coralswarm.example",
            "https://api.coralswarm.example/v1/email/unsubscribe?t=tok",
        );
        assert!(!html.contains("captured whether or not"));
    }

    #[test]
    fn body_names_no_specific_platform() {
        let html = render_connect_nudge_email_html(
            "person@example.com",
            "https://acme.coralswarm.example/settings/organization/connectors",
            "https://acme.coralswarm.example",
            "https://api.coralswarm.example/v1/email/unsubscribe?t=tok",
        );
        let lower = html.to_lowercase();
        for platform in ["slack", "teams", "gmail", "outlook", "google chat", "meet"] {
            assert!(!lower.contains(platform), "must not promise a specific platform: {platform}");
        }
    }

    #[test]
    fn dynamic_values_are_escaped() {
        let html = render_connect_nudge_email_html(
            "a<b>@example.com",
            "https://acme.coralswarm.example/settings/organization/connectors?x=1&y=2",
            "https://acme.coralswarm.example",
            "https://api.coralswarm.example/v1/email/unsubscribe?t=a&b",
        );
        assert!(!html.contains("a<b>@example.com"), "raw email must not appear unescaped");
        assert!(html.contains("a&lt;b&gt;@example.com"));
        assert!(html.contains("t=a&amp;b"));
    }

    #[test]
    fn body_contains_a_visible_unsubscribe_link() {
        let html = render_connect_nudge_email_html(
            "person@example.com",
            "https://acme.coralswarm.example/settings/organization/connectors",
            "https://acme.coralswarm.example",
            "https://api.coralswarm.example/v1/email/unsubscribe?t=abc123",
        );
        assert!(html.contains("href=\"https://api.coralswarm.example/v1/email/unsubscribe?t=abc123\""));
    }

    #[test]
    fn escape_html_still_covers_all_five_entities() {
        assert_eq!(escape_html("<>&\"'"), "&lt;&gt;&amp;&quot;&#39;");
    }
}

#[cfg(test)]
mod join_org_copy_tests {
    //! Pure, DB-and-network-free — same reasoning as `copy_tests` above
    //! (CS-440, org-onboarding plan §3 item 29, D19).

    use super::{escape_html, render_join_org_email_html};

    /// The audience predicate itself, stated in the body: an existing
    /// CoralSwarm account, a shared Slack channel, a request rather than a
    /// signup. FAILS WITHOUT THE FIX if a future edit drops the "not a new
    /// signup" honesty line.
    #[test]
    fn body_states_this_is_a_join_request_not_a_signup() {
        let html = render_join_org_email_html(
            "person@example.com",
            "Acme",
            "https://app.coralswarm.example/join/org_acme?t=tok",
            "https://app.coralswarm.example",
            "https://api.coralswarm.example/v1/email/unsubscribe?t=tok",
        );
        assert!(
            html.contains("not a new signup"),
            "must plainly say this is a join request against an existing account, not a signup"
        );
        assert!(html.contains("Acme"), "must name the org");
    }

    /// House rule (this story's brief, verbatim): the recipient must never
    /// learn the pool state or a prior rejection. FAILS WITHOUT THE FIX if a
    /// future edit reintroduces either.
    #[test]
    fn body_never_discloses_pool_state_or_rejection() {
        let html = render_join_org_email_html(
            "person@example.com",
            "Acme",
            "https://app.coralswarm.example/join/org_acme?t=tok",
            "https://app.coralswarm.example",
            "https://api.coralswarm.example/v1/email/unsubscribe?t=tok",
        );
        let lower = html.to_lowercase();
        for forbidden in ["reject", "declin", "full", "waitlist", "pool"] {
            assert!(!lower.contains(forbidden), "must never mention: {forbidden}");
        }
    }

    /// House rule (this story's brief, verbatim): "slot" is the billing
    /// place, "seat" is team-ocean access — this email is about org
    /// membership, neither of those two concepts.
    #[test]
    fn body_never_says_seat_or_slot() {
        let html = render_join_org_email_html(
            "person@example.com",
            "Acme",
            "https://app.coralswarm.example/join/org_acme?t=tok",
            "https://app.coralswarm.example",
            "https://api.coralswarm.example/v1/email/unsubscribe?t=tok",
        );
        let lower = html.to_lowercase();
        assert!(!lower.contains("seat"), "must never say 'seat' (D15)");
        assert!(!lower.contains("slot"), "must never say 'slot' (D15)");
    }

    /// D6: one clear, one-line CTA naming the org — the button text itself.
    #[test]
    fn body_has_a_one_line_cta_naming_the_org() {
        let html = render_join_org_email_html(
            "person@example.com",
            "Acme",
            "https://app.coralswarm.example/join/org_acme?t=tok",
            "https://app.coralswarm.example",
            "https://api.coralswarm.example/v1/email/unsubscribe?t=tok",
        );
        assert!(
            html.contains(">Join Acme on coralswarm<"),
            "the CTA button text must be the one-line 'Join {{org}} on coralswarm' (D6)"
        );
    }

    #[test]
    fn body_contains_a_visible_unsubscribe_link() {
        let html = render_join_org_email_html(
            "person@example.com",
            "Acme",
            "https://app.coralswarm.example/join/org_acme?t=tok",
            "https://app.coralswarm.example",
            "https://api.coralswarm.example/v1/email/unsubscribe?t=abc123",
        );
        assert!(html.contains("href=\"https://api.coralswarm.example/v1/email/unsubscribe?t=abc123\""));
    }

    #[test]
    fn dynamic_values_are_escaped() {
        let html = render_join_org_email_html(
            "a<b>@example.com",
            "Acme & Co",
            "https://app.coralswarm.example/join/org_acme?t=tok&x=1",
            "https://app.coralswarm.example",
            "https://api.coralswarm.example/v1/email/unsubscribe?t=a&b",
        );
        assert!(!html.contains("a<b>@example.com"), "raw email must not appear unescaped");
        assert!(html.contains("a&lt;b&gt;@example.com"));
        assert!(html.contains("Acme &amp; Co"));
        assert!(html.contains("t=a&amp;b"));
    }

    #[test]
    fn escape_html_still_covers_all_five_entities() {
        assert_eq!(escape_html("<>&\"'"), "&lt;&gt;&amp;&quot;&#39;");
    }
}

#[cfg(test)]
mod send_nudge_email_tests {
    //! Network-but-no-database tests for [`send_nudge_email`] — a real HTTP
    //! POST against a local mock Resend server (`RESEND_API_BASE_URL`,
    //! same seam `routes::org_invites`'s own pg_tests use), no
    //! `ORGT_TEST_BASE_URL` needed since nothing here touches Postgres.

    use super::*;
    use std::sync::{Arc as StdArc, Mutex as StdMutex};

    // `super::RESEND_BASE_URL_LOCK`, not a lock private to this module —
    // see that static.

    /// Spawn a local mock Resend server that captures the JSON body of the
    /// first request it receives and always answers `200 {"id":"mock"}` —
    /// [`send_nudge_email`] only branches on 2xx-vs-not, so a fixed success
    /// response is sufficient. Returns the base URL and the captured-body
    /// handle.
    async fn spawn_mock_resend() -> (String, StdArc<StdMutex<Option<serde_json::Value>>>) {
        use axum::{routing::post, Json, Router};

        let captured: StdArc<StdMutex<Option<serde_json::Value>>> = StdArc::new(StdMutex::new(None));
        let captured_for_handler = captured.clone();
        let handler = move |Json(body): Json<serde_json::Value>| {
            let captured = captured_for_handler.clone();
            async move {
                *captured.lock().unwrap() = Some(body);
                Json(serde_json::json!({ "id": "mock" }))
            }
        };
        let app = Router::new().route("/emails", post(handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock resend listener");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        (format!("http://{addr}"), captured)
    }

    /// Minimal `AppState` with a real, distinguishable API-vs-web apex split
    /// (T1's `TenantRuntime::new_with_web_apex`) — `api_base_url()` and
    /// `personal_web_base_url()` MUST resolve to genuinely different hosts
    /// for this test to mean anything.
    fn test_state(resend_api_key: &str) -> AppState {
        let resolver =
            crate::tenancy::secrets::StaticMapResolver::from_pairs(Vec::<(&str, &str)>::new());
        let registry = std::sync::Arc::new(crate::tenancy::registry::TenantRegistry::new(
            std::sync::Arc::new(resolver),
        ));
        let tenant_runtime = crate::tenancy::runtime::TenantRuntime::new_with_web_apex(
            true,
            "api.coralswarm.test",
            "coralswarm.test",
            registry,
        );
        AppState {
            db: None,
            store: None,
            resend_api_key: Some(resend_api_key.to_string()),
            r2: None,
            http_client: reqwest::Client::new(),
            embedder: StdArc::new(crate::StubEmbedder),
            llm: None,
            rate_limiter: StdArc::new(crate::rate_limit::InMemoryRateLimiter::new()),
            admission: Default::default(),
            in_flight: StdArc::new(Default::default()),
            metrics: StdArc::new(crate::telemetry::NoopMetrics),
            dek_cache: crate::dek_cache::DekCache::new(),
            connector_flag_cache: crate::connector_flags::ConnectorFlagCache::new(),
            tenant_runtime: Some(tenant_runtime),
            env: crate::AppEnv::default(),
            task_tracker: tokio_util::task::TaskTracker::new(),
            worker_activity: crate::workers::tenant_harness::TenantActivity::new(),
            mcp_cancellation_token: tokio_util::sync::CancellationToken::new(),
            mcp_task_manager: crate::mcp::server::McpTaskRegistry::new(),
            mcp_ask_in_flight: crate::mcp::server::AskInFlight::new(),
            stripe: None,
        }
    }

    /// FAILS WITHOUT THE FIX (CodeRabbit round 1, #1005, Functional
    /// Correctness): an earlier version built the unsubscribe URL from
    /// `web_base` (the WEB APP's own origin, `https://app.{web_apex}`) —
    /// a host `/v1/email/unsubscribe` (a BACKEND route) is never mounted
    /// on. Proves the actual outbound Resend payload's `List-Unsubscribe`
    /// header and HTML body both carry the API origin instead.
    #[tokio::test]
    async fn unsubscribe_url_uses_the_api_origin_not_the_web_origin() {
        let _guard = super::RESEND_BASE_URL_LOCK.lock().unwrap();
        let (resend_base, captured) = spawn_mock_resend().await;
        // SAFETY: serialized by `_guard` above, restored before it drops.
        unsafe {
            std::env::set_var("RESEND_API_BASE_URL", &resend_base);
        }

        let state = test_state("re_mock_key");
        let sent = send_nudge_email(
            &state,
            "person@example.com",
            "https://app.coralswarm.test", // web_base — the SIGN-UP link's own host
            "unsub-tok-123",
            "prefill-tok-456",
        )
        .await;

        // SAFETY: still serialized by `_guard`.
        unsafe {
            std::env::remove_var("RESEND_API_BASE_URL");
        }
        assert!(sent, "the mock server answers 2xx, so this must report success");

        let body = captured.lock().unwrap().take().expect("mock resend must have received a request");
        let html = body["html"].as_str().expect("payload must carry an html field");
        assert!(
            html.contains("https://api.coralswarm.test/v1/email/unsubscribe?t=unsub-tok-123"),
            "the in-body unsubscribe link must use the API origin, not the web origin: {html}"
        );
        assert!(
            !html.contains("https://app.coralswarm.test/v1/email/unsubscribe"),
            "the in-body unsubscribe link must NOT point at the web app's own origin — that \
             route is not mounted there: {html}"
        );
        // The sign-up link, by contrast, MUST still use web_base.
        assert!(
            html.contains("https://app.coralswarm.test/signup?pt=prefill-tok-456"),
            "the sign-up link must still use the web origin: {html}"
        );

        let headers = body["headers"].as_object().expect("payload must carry a headers object");
        let list_unsubscribe = headers["List-Unsubscribe"].as_str().unwrap();
        assert!(
            list_unsubscribe.contains("https://api.coralswarm.test/v1/email/unsubscribe"),
            "the List-Unsubscribe header must use the API origin too: {list_unsubscribe}"
        );
    }
}

#[cfg(test)]
mod unsubscribe_pg_tests {
    //! Live-Postgres proof of the suppression list's data layer, run against
    //! a scratch database this module creates and tears down itself — same
    //! harness shape as `connectors::slack_unmatched_email`'s own
    //! `discover_unmatched_recipients_pg_tests`, the original this pattern
    //! was copied from.
    //!
    //! Proves, in order: a token round-trips end to end (claim -> suppress
    //! -> `suppressed_at` set); an account that LOOKS like an org account
    //! (`clerk_org_id` set) round-trips identically, with no plane check
    //! anywhere in the path to quietly reintroduce (the regression this
    //! module's "Control-plane only, not per-plane" doc section names); a
    //! suppressed recipient is never reclaimed (so never emailed again); an
    //! unknown token changes nothing and reports the SAME outcome shape as a
    //! real one; and the suppression check cannot be raced — two concurrent
    //! claims for the identical recipient still yield at most one token to
    //! send with.
    //!
    //!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //!     cargo test -p coralswarm-backend --lib unsubscribe_pg_tests \
    //!     -- --test-threads=1

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use uuid::Uuid;

    use super::{claim_notice_slot, resolve_prefill_token, suppress_by_token};

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn recreate(base: &str, db: &str) -> PgConnection {
        let mut conn = PgConnection::connect(base).await.expect("connect admin");
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        conn
    }
    async fn setup(db: &str) -> Option<(PgConnection, PgPool)> {
        let base = base_url()?;
        let admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        Some((admin_conn, pool))
    }
    async fn teardown(mut admin_conn: PgConnection, pool: PgPool, db: &str) {
        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// A minimal, owned account — `accounts.owner_user_id` is `NOT NULL`,
    /// same seeding order every other pg suite in this crate uses.
    async fn seed_account(pool: &PgPool) -> Uuid {
        let owner: Uuid = sqlx::query_scalar(
            "INSERT INTO users(email) VALUES ('owner+' || gen_random_uuid() || '@example.com') RETURNING id",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(owner)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// An ORG account — same shape as [`seed_account`], plus a non-NULL
    /// `clerk_org_id` (`uq_accounts_clerk_org_id`, migration `0027`), which is
    /// the ONLY thing that distinguishes an org account from a personal one
    /// anywhere in this crate (`routes::slack::account_plane`'s own
    /// definition). Neither `claim_notice_slot` nor `suppress_by_token` reads
    /// this column at all — that absence is the point of
    /// [`org_account_notice_is_resolvable_and_suppressible_pg`] below.
    async fn seed_org_account(pool: &PgPool) -> Uuid {
        let owner: Uuid = sqlx::query_scalar(
            "INSERT INTO users(email) VALUES ('owner+' || gen_random_uuid() || '@example.com') RETURNING id",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id) \
             VALUES ($1, 'org_' || gen_random_uuid()) RETURNING id",
        )
        .bind(owner)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// FAILS WITHOUT THE FIX: this is the regression the founder's
    /// correction named directly — "a feature that only works for one of
    /// them is not done". The ORIGINAL bug was never in this module: both
    /// functions here always took a bare pool/executor with no opinion on
    /// which plane it belonged to. The bug was in the CALLER
    /// (`workers::slack_unmatched_email::process_job`), which opened its
    /// claim transaction on `t.pool` — an ORG account's own tenant database
    /// in production, a physically different Postgres instance than the one
    /// `suppress_by_token` (and the public route that calls it) can ever
    /// see. An org account's row was therefore claimable but never
    /// resolvable, and this suite's existing tests — which only ever seed a
    /// bare (plane-less) account into ONE pool — could not have caught that,
    /// because they never modelled a second pool at all.
    ///
    /// What this test pins instead, now that the fix is "always claim on
    /// `control_pool`" (this module's own "Control-plane only, not
    /// per-plane" doc section): an account that LOOKS like an org account
    /// (`clerk_org_id` set, exactly like `routes::slack::account_plane`
    /// would classify it) round-trips through `claim_notice_slot` ->
    /// `suppress_by_token` — the same two calls the public route drives for
    /// every recipient — with IDENTICAL behaviour to
    /// `token_round_trips_from_claim_to_suppressed_pg`'s personal account
    /// above. Nothing here branches on `clerk_org_id`, which is exactly the
    /// property the fix depends on: there is no plane check left ANYWHERE in
    /// this table's read/write path for a reintroduced per-plane pool choice
    /// to quietly break again.
    #[tokio::test]
    async fn org_account_notice_is_resolvable_and_suppressible_pg() {
        let db = "cs_unsub_org_account_test";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping org_account_notice_is_resolvable_and_suppressible_pg — no ORGT_TEST_BASE_URL");
            return;
        };
        let org_account_id = seed_org_account(&pool).await;

        let tokens = claim_notice_slot(&pool, org_account_id, "slack", "T-org", "C-org", "person@acme-org.example")
            .await
            .unwrap()
            .expect("an org account's recipient must be claimable exactly like a personal one");

        let found = suppress_by_token(&pool, &tokens.unsubscribe_token).await.unwrap();
        assert!(
            found,
            "the public unsubscribe route's own lookup must resolve an org account's token, \
             not just a personal account's"
        );

        let is_suppressed: bool = sqlx::query_scalar(
            "SELECT suppressed_at IS NOT NULL FROM slack_unmatched_email_notices \
             WHERE unsubscribe_token = $1",
        )
        .bind(&tokens.unsubscribe_token)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(is_suppressed, "an org account's row must be suppressible, same as a personal account's");

        // Same property, for the SAME reason, on the prefill-token side
        // (CodeRabbit round 1, #1005): an org account's row must resolve
        // exactly like a personal one's — no plane check anywhere in this
        // read path either.
        let resolved_email = resolve_prefill_token(&pool, &tokens.prefill_token)
            .await
            .unwrap();
        assert_eq!(
            resolved_email.as_deref(),
            Some("person@acme-org.example"),
            "an org account's prefill token must resolve to its recipient's email, not just a \
             personal account's"
        );

        teardown(admin_conn, pool, db).await;
    }

    /// FAILS WITHOUT THE FIX: proves the whole round trip a real send/click
    /// takes — `claim_notice_slot` mints a token, `suppress_by_token`
    /// resolves it back to the same row (via the control-only SECURITY
    /// DEFINER bootstrap function, migration 0228) and sets `suppressed_at`.
    #[tokio::test]
    async fn token_round_trips_from_claim_to_suppressed_pg() {
        let db = "cs_unsub_roundtrip_test";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping token_round_trips_from_claim_to_suppressed_pg — no ORGT_TEST_BASE_URL");
            return;
        };
        let account_id = seed_account(&pool).await;

        let tokens = claim_notice_slot(&pool, account_id, "slack", "T1", "C1", "person@example.com")
            .await
            .unwrap()
            .expect("a brand-new recipient must be claimable");
        assert_ne!(
            tokens.unsubscribe_token, tokens.prefill_token,
            "the two tokens must be independent random values, never the same one reused"
        );

        let found = suppress_by_token(&pool, &tokens.unsubscribe_token).await.unwrap();
        assert!(found, "a real token must resolve and be marked suppressed");

        let is_suppressed: bool = sqlx::query_scalar(
            "SELECT suppressed_at IS NOT NULL FROM slack_unmatched_email_notices \
             WHERE unsubscribe_token = $1",
        )
        .bind(&tokens.unsubscribe_token)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(is_suppressed, "suppressed_at must be set after a successful unsubscribe");

        teardown(admin_conn, pool, db).await;
    }

    /// FAILS WITHOUT THE FIX (CodeRabbit round 1, #1005): proves the signup
    /// link's opaque token round-trips back to the exact email it was minted
    /// for — the property `routes::email_prefill` depends on to prefill the
    /// signup form without ever putting the address itself in a URL.
    #[tokio::test]
    async fn prefill_token_resolves_to_the_claimed_email_pg() {
        let db = "cs_unsub_prefill_roundtrip_test";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping prefill_token_resolves_to_the_claimed_email_pg — no ORGT_TEST_BASE_URL");
            return;
        };
        let account_id = seed_account(&pool).await;

        let tokens = claim_notice_slot(&pool, account_id, "slack", "T1", "C1", "prefill-target@example.com")
            .await
            .unwrap()
            .expect("a brand-new recipient must be claimable");

        let resolved = resolve_prefill_token(&pool, &tokens.prefill_token).await.unwrap();
        assert_eq!(
            resolved.as_deref(),
            Some("prefill-target@example.com"),
            "the prefill token must resolve back to the exact email it was minted for"
        );

        teardown(admin_conn, pool, db).await;
    }

    /// FAILS WITHOUT THE FIX: enumeration resistance on the resolve side too
    /// — an unknown/never-issued prefill token must return `None`, the same
    /// generic outcome `routes::email_prefill` gives for every other
    /// refusal, never a distinguishable error a caller could branch on.
    #[tokio::test]
    async fn unknown_prefill_token_returns_none_pg() {
        let db = "cs_unsub_prefill_unknown_test";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping unknown_prefill_token_returns_none_pg — no ORGT_TEST_BASE_URL");
            return;
        };

        let resolved = resolve_prefill_token(&pool, "this-token-was-never-issued").await.unwrap();
        assert!(resolved.is_none(), "a token matching no row must resolve to None, not error");

        teardown(admin_conn, pool, db).await;
    }

    /// FAILS WITHOUT THE FIX: enumeration resistance depends on an unknown
    /// token returning the SAME shape as a valid one, not erroring or
    /// panicking differently. `Ok(false)` (rather than a distinct error
    /// variant a route could accidentally branch on) is that shape.
    #[tokio::test]
    async fn unknown_token_returns_false_pg() {
        let db = "cs_unsub_unknown_test";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping unknown_token_returns_false_pg — no ORGT_TEST_BASE_URL");
            return;
        };

        let found = suppress_by_token(&pool, "this-token-was-never-issued").await.unwrap();
        assert!(!found, "a token matching no row must report false, not error");

        teardown(admin_conn, pool, db).await;
    }

    /// FAILS WITHOUT THE FIX: this is the whole point of the suppression
    /// list — once unsubscribed, `claim_notice_slot` must refuse to claim
    /// the SAME recipient again even once the ordinary 7-day cooldown has
    /// long since elapsed (backdated here so cooldown expiry is not what's
    /// being tested — only the suppression gate is).
    #[tokio::test]
    async fn suppressed_recipient_is_never_reclaimed_pg() {
        let db = "cs_unsub_reclaim_test";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping suppressed_recipient_is_never_reclaimed_pg — no ORGT_TEST_BASE_URL");
            return;
        };
        let account_id = seed_account(&pool).await;

        let tokens = claim_notice_slot(&pool, account_id, "slack", "T1", "C1", "person@example.com")
            .await
            .unwrap()
            .expect("first claim must succeed");
        suppress_by_token(&pool, &tokens.unsubscribe_token).await.unwrap();

        // Backdate past the cooldown so a refusal below can only be the
        // suppression gate, not an ordinary in-cooldown refusal.
        sqlx::query(
            "UPDATE slack_unmatched_email_notices \
             SET last_notified_at = now() - INTERVAL '30 days' \
             WHERE unsubscribe_token = $1",
        )
        .bind(&tokens.unsubscribe_token)
        .execute(&pool)
        .await
        .unwrap();

        let reclaimed = claim_notice_slot(&pool, account_id, "slack", "T1", "C1", "person@example.com")
            .await
            .unwrap();
        assert!(
            reclaimed.is_none(),
            "a suppressed recipient must never be reclaimed, even outside the cooldown window"
        );

        teardown(admin_conn, pool, db).await;
    }

    /// FAILS WITHOUT THE FIX: the suppression check is folded into the same
    /// `ON CONFLICT ... WHERE` statement as the cooldown check specifically
    /// so it cannot race a claim. Two callers racing the identical recipient
    /// must still see the database's usual serialization on the unique
    /// index: exactly one comes back with a token, the other with `None`.
    #[tokio::test]
    async fn concurrent_claims_yield_at_most_one_token_pg() {
        let db = "cs_unsub_race_test";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping concurrent_claims_yield_at_most_one_token_pg — no ORGT_TEST_BASE_URL");
            return;
        };
        let account_id = seed_account(&pool).await;

        let (a, b) = tokio::join!(
            claim_notice_slot(&pool, account_id, "slack", "T1", "C1", "racer@example.com"),
            claim_notice_slot(&pool, account_id, "slack", "T1", "C1", "racer@example.com"),
        );
        let winners = [a.unwrap(), b.unwrap()].into_iter().filter(Option::is_some).count();
        assert_eq!(winners, 1, "exactly one of two concurrent claims for the same recipient must win");

        teardown(admin_conn, pool, db).await;
    }
}

#[cfg(test)]
mod migration_0230_collapse_pg_tests {
    //! Live-Postgres proof of migration `0230`'s own duplicate-collapse
    //! behavior (D4, `docs/plans/org-onboarding-from-the-invite.md` §3 item
    //! 2, CS-427): feed it two pre-existing rows shaped like the OLD
    //! `(account_id, provider, email)` key (two different accounts, the SAME
    //! recipient — the exact shape that key allowed and `(provider, email)`
    //! does not), then re-run migration `0230`'s OWN SQL file, read off disk
    //! verbatim rather than hand-copied into this test (so this test cannot
    //! silently drift from what actually ships), and prove exactly one row
    //! survives: the one with the most recent `last_notified_at`.
    //!
    //! This test's own database is migrated all the way through `0230` by
    //! `setup` (so the `(provider, email)` constraint already exists) — the
    //! two conflicting rows are only reachable by dropping that constraint
    //! first, which is what "pre-existing data from before this migration
    //! ran" actually looked like on a real, already-deployed database the
    //! FIRST time `0230` runs there. Re-running the exact same migration
    //! file's SQL a second time is also migration `0230`'s own documented
    //! idempotency contract (see its header).
    //!
    //!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //!     cargo test -p coralswarm-backend --lib migration_0230_collapse_pg_tests

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use uuid::Uuid;

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn recreate(base: &str, db: &str) -> PgConnection {
        let mut conn = PgConnection::connect(base).await.expect("connect admin");
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        conn
    }
    async fn setup(db: &str) -> Option<(PgConnection, PgPool)> {
        let base = base_url()?;
        let admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        Some((admin_conn, pool))
    }
    async fn teardown(mut admin_conn: PgConnection, pool: PgPool, db: &str) {
        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
    async fn seed_account(pool: &PgPool) -> Uuid {
        let owner: Uuid = sqlx::query_scalar(
            "INSERT INTO users(email) VALUES ('owner+' || gen_random_uuid() || '@example.com') RETURNING id",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(owner)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// FAILS WITHOUT THE FIX: two rows for the same `(provider, email)`,
    /// seeded under different accounts and different `last_notified_at`
    /// values, must collapse to exactly the more recent one once migration
    /// `0230`'s own SQL runs.
    #[tokio::test]
    async fn duplicate_rows_for_the_same_person_collapse_to_the_most_recent_pg() {
        let db = "cs_migration_0230_collapse_test";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!(
                "skipping duplicate_rows_for_the_same_person_collapse_to_the_most_recent_pg — \
                 no ORGT_TEST_BASE_URL"
            );
            return;
        };
        let account_older = seed_account(&pool).await;
        let account_newer = seed_account(&pool).await;
        let email = "dup@example.com";

        // Simulate the OLD, pre-0230 shape: relax the constraint `setup`'s
        // full migration run already installed, so two rows for the SAME
        // (provider, email) — one per account — can coexist, exactly like a
        // real database's data looked the moment before 0230 first ran
        // there.
        sqlx::query(
            "ALTER TABLE slack_unmatched_email_notices \
             DROP CONSTRAINT IF EXISTS slack_unmatched_email_notices_provider_email_key",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO slack_unmatched_email_notices \
             (account_id, provider, email, workspace_id, channel_id, last_notified_at) \
             VALUES ($1, 'slack', $2, 'T1', 'C1', now() - INTERVAL '3 days')",
        )
        .bind(account_older)
        .bind(email)
        .execute(&pool)
        .await
        .unwrap();
        let newer_id: Uuid = sqlx::query_scalar(
            "INSERT INTO slack_unmatched_email_notices \
             (account_id, provider, email, workspace_id, channel_id, last_notified_at) \
             VALUES ($1, 'slack', $2, 'T2', 'C2', now()) RETURNING id",
        )
        .bind(account_newer)
        .bind(email)
        .fetch_one(&pool)
        .await
        .unwrap();

        let count_before: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM slack_unmatched_email_notices WHERE email = $1",
        )
        .bind(email)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count_before, 2, "the test setup must actually create two conflicting rows");

        // Re-run migration 0230's OWN file, verbatim, off disk — never a
        // hand-copied duplicate of its SQL.
        let migration_path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/migrations_control/0230_slack_unmatched_email_notices_per_person_cooldown.sql"
        );
        let migration_sql = std::fs::read_to_string(migration_path)
            .unwrap_or_else(|e| panic!("reading {migration_path}: {e}"));
        sqlx::raw_sql(&migration_sql)
            .execute(&pool)
            .await
            .expect("re-running migration 0230's own SQL must succeed");

        let remaining: Vec<Uuid> = sqlx::query_scalar(
            "SELECT id FROM slack_unmatched_email_notices WHERE email = $1",
        )
        .bind(email)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            remaining,
            vec![newer_id],
            "only the row with the most recent last_notified_at must survive the collapse"
        );

        let survivor_account: Uuid = sqlx::query_scalar(
            "SELECT account_id FROM slack_unmatched_email_notices WHERE id = $1",
        )
        .bind(newer_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            survivor_account, account_newer,
            "account_id stays a plain column on the surviving row — it is not dropped, only \
             removed from the uniqueness key"
        );

        let has_new_constraint: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_constraint \
             WHERE conname = 'slack_unmatched_email_notices_provider_email_key')",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            has_new_constraint,
            "the (provider, email) UNIQUE constraint must be restored once the migration reruns"
        );

        teardown(admin_conn, pool, db).await;
    }
}
