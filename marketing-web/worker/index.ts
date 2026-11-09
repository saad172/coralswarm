/**
 * CoralSwarm marketing site — Worker with static assets.
 *
 * This Worker sits in front of the static export (`./out`, bound below as
 * `ASSETS`). It handles a small number of routes itself and falls through to
 * the Asset Worker for everything else. `run_worker_first` in wrangler.jsonc
 * lists exactly which paths reach this code — every other request (all the
 * HTML/CSS/JS/image assets) is served straight from ASSETS without invoking
 * the Worker at all.
 *
 *   POST /api/contact              the v3 contact form (waitlist + inbox)
 *   GET|POST /api/waitlist/unsubscribe   one-click unsubscribe
 *   POST /api/waves                first-party cookieless pageview log
 *   GET  /api/seo-stats            token-gated stats for the SEO watchdog
 *   GET  /opengraph-image          content-type fix, see below
 *
 * /api/waves is named the way it is on purpose — see handlePageView below.
 *
 * Deliberately isolated from the main CoralSwarm product backend — see
 * marketing-web's project instructions for why.
 *
 * NOTE: `POST /api/waitlist` — the old email-only endpoint — is gone. It had
 * no bot protection beyond a per-IP rate limit, so leaving it up would have
 * been a Turnstile-free side door into the same table and the same mailbox,
 * making the check on /api/contact pointless. Nothing posts to it any more:
 * the component that did (components/EmailForm.tsx) was deleted with the v1
 * page. The unsubscribe route stays, because links in already-delivered
 * confirmation emails point at it forever.
 */

export interface Env {
  DB: D1Database;
  ASSETS: Fetcher;
  WAITLIST_RATE_LIMITER: RateLimit;
  PAGEVIEW_RATE_LIMITER: RateLimit;
  EMAIL: SendEmail;
  // HMAC signing key for unsubscribe links — set via `wrangler secret put
  // UNSUBSCRIBE_SECRET`. Not a third-party credential, just a random value
  // used to prove a /api/waitlist/unsubscribe request actually came from a
  // link this Worker generated, so a guessed email address alone can't
  // remove someone else from the list.
  UNSUBSCRIBE_SECRET: string;
  // Turnstile server-side key — `wrangler secret put TURNSTILE_SECRET_KEY`.
  // Cloudflare's documented always-passes test secret is
  // 1x0000000000000000000000000000000AA, which pairs with the test site key
  // the frontend falls back to in development.
  TURNSTILE_SECRET_KEY: string;
  // Where contact submissions are emailed. Plain var, not a secret.
  CONTACT_NOTIFY_TO: string;
  // Bearer token the SEO watchdog agent presents on GET /api/seo-stats — set
  // via `wrangler secret put SEO_STATS_TOKEN` from marketing-web/. A random
  // value with no other meaning; rotate by setting a new one and updating
  // the watchdog's config, no code change needed.
  SEO_STATS_TOKEN: string;
  // Full Google service-account JSON (the file downloaded from the GCP
  // console, unmodified — this Worker reads client_email and private_key
  // out of it) for a service account that has been added as a verified
  // owner/user of the `sc-domain:coralswarm.example` property in Search
  // Console. Set via `wrangler secret put GSC_SERVICE_ACCOUNT_KEY` from
  // marketing-web/ (paste the whole JSON file as the value). Never logged.
  GSC_SERVICE_ACCOUNT_KEY: string;
}

// Basic, intentionally-not-exhaustive email shape check. We don't need to
// validate deliverability here — just filter out obvious garbage before it
// hits the database.
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

// First-touch attribution fields sent by the contact form (see
// components/firstTouch.ts). All of it is attacker-controlled — it arrives
// on an unauthenticated POST body — so every field is validated/sanitized
// below before it ever reaches a bound param, let alone the database.
const MAX_ATTRIBUTION_FIELD_LEN = 256;
const MAX_NAME_LEN = 200;
const MAX_MESSAGE_LEN = 4000;
const MAX_EMAIL_LEN = 320;

/**
 * Non-strings are dropped (null) rather than coerced/stringified — a
 * malicious or buggy client could otherwise send an object/array and have
 * it serialize into something like "[object Object]". Strings are trimmed
 * and capped so a client can't stuff an unbounded blob into a D1 column.
 */
function sanitizeAttributionField(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  if (trimmed.length === 0) return null;
  return trimmed.slice(0, MAX_ATTRIBUTION_FIELD_LEN);
}

/** Same rules, caller-chosen cap — for the form's own text fields. */
function sanitizeText(value: unknown, max: number): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  if (trimmed.length === 0) return null;
  return trimmed.slice(0, max);
}

// CF-IPCountry is set by Cloudflare's edge on every request that reaches
// this Worker (coralswarm.example is proxied through Cloudflare — see the
// `routes` entry in wrangler.jsonc) — it is not client-supplied and can't
// be spoofed by the POST body. Still validated defensively rather than
// trusted blindly. Values are ISO 3166-1 alpha-2 codes plus a couple of
// Cloudflare specials ("T1" = Tor exit node, "XX" = unknown, "EU" = certain
// EU institutional ranges) — all two characters, so a simple shape check
// is enough. Note: only the coarse country code is stored — never an IP
// address.
const COUNTRY_RE = /^[A-Z0-9]{2}$/;

function sanitizeCountry(value: string | null): string | null {
  if (!value) return null;
  return COUNTRY_RE.test(value) ? value : null;
}

/**
 * Escape a user-supplied string for interpolation into an HTML email body.
 *
 * The notification email below is assembled by string concatenation and
 * carries three attacker-controlled fields (name, email, message) straight
 * into a mailbox a human reads. Without this, a submitter could inject
 * markup and links into mail that appears to come from our own domain.
 * Ampersand first, or it would double-escape the entities added after it.
 */
function escapeHtml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function json(body: unknown, init?: ResponseInit): Response {
  return new Response(JSON.stringify(body), {
    ...init,
    headers: {
      "content-type": "application/json; charset=utf-8",
      ...(init?.headers ?? {}),
    },
  });
}

function html(body: string, init?: ResponseInit): Response {
  return new Response(body, {
    ...init,
    headers: {
      "content-type": "text/html; charset=utf-8",
      ...(init?.headers ?? {}),
    },
  });
}

async function computeUnsubscribeToken(email: string, secret: string): Promise<string> {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const signature = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(email));
  return Array.from(new Uint8Array(signature))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("")
    .slice(0, 32); // 128 bits — plenty for this threat model (griefing, not a real secret)
}

/**
 * Verify a Turnstile token with Cloudflare.
 *
 * Fails CLOSED when TURNSTILE_SECRET_KEY is unset. The tempting
 * alternative — log a warning and let the request through — would mean a
 * misconfigured deploy silently ships an unprotected form that still looks
 * like it has a bot check. A form that is loudly broken gets fixed in
 * minutes; one that is quietly open does not get noticed at all.
 */
async function verifyTurnstile(
  token: unknown,
  ip: string,
  env: Env
): Promise<{ ok: true } | { ok: false; status: number; error: string }> {
  if (!env.TURNSTILE_SECRET_KEY) {
    console.error("TURNSTILE_SECRET_KEY is not set — refusing to accept submissions");
    return {
      ok: false,
      status: 503,
      error: "The form is temporarily unavailable. Please try again shortly.",
    };
  }

  if (typeof token !== "string" || token.length === 0 || token.length > 2048) {
    return { ok: false, status: 400, error: "Verification failed. Please reload and try again." };
  }

  const body = new FormData();
  body.append("secret", env.TURNSTILE_SECRET_KEY);
  body.append("response", token);
  if (ip !== "unknown") body.append("remoteip", ip);

  try {
    const resp = await fetch("https://challenges.cloudflare.com/turnstile/v0/siteverify", {
      method: "POST",
      body,
    });
    const outcome = (await resp.json()) as { success?: boolean; "error-codes"?: string[] };
    if (!outcome.success) {
      console.error("turnstile rejected", outcome["error-codes"]);
      return { ok: false, status: 400, error: "Verification failed. Please reload and try again." };
    }
    return { ok: true };
  } catch (err) {
    // Can't verify => can't accept. Same reasoning as the missing-key case.
    console.error("turnstile verify threw", err);
    return {
      ok: false,
      status: 503,
      error: "Couldn't verify that request. Please try again in a moment.",
    };
  }
}

// Logo for the confirmation email, served from the same static export this
// Worker already fronts (`public/coralswarm-logo.png` -> `./out/`) and fetched
// through the `ASSETS` binding rather than bundled/base64'd into the Worker.
// Sent as a real inline attachment (`disposition: "inline"` + `contentId`,
// referenced in the HTML as `cid:waitlist-logo`) — not a `data:` URI, which
// Gmail and several other major clients strip from HTML email bodies.
const LOGO_PATH = "/coralswarm-logo.png";
const LOGO_CONTENT_ID = "waitlist-logo";

async function fetchLogoAttachment(env: Env): Promise<EmailAttachment | null> {
  try {
    const resp = await env.ASSETS.fetch(`https://coralswarm.example${LOGO_PATH}`);
    if (!resp.ok) {
      console.error("logo asset fetch failed", resp.status);
      return null;
    }
    return {
      disposition: "inline",
      contentId: LOGO_CONTENT_ID,
      filename: "coralswarm.png",
      type: "image/png",
      content: await resp.arrayBuffer(),
    };
  } catch (err) {
    // Missing/unfetchable logo shouldn't block the confirmation email.
    console.error("logo asset fetch threw", err);
    return null;
  }
}

// Physical postal address for the footer. Required by CAN-SPAM for
// commercial mail and expected by most inbox providers' bulk-sender
// guidance. Left empty deliberately rather than shipping a placeholder:
// when this is "", the address line is omitted entirely instead of sending
// a wrong address to real people. Fill in the real registered address.
// TODO(founder): set the real registered postal address before launch.
const POSTAL_ADDRESS = "";

// Footer signup date, rendered lowercase and coarse ("31 jul 2026") per the
// design system's mono/meta voice. Confirmation is sent inline with the
// signup, so "now" is the signup date. UTC so the rendered date doesn't
// depend on which Cloudflare colo happened to serve the request.
const MONTHS_LOWER = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"];

function formatSignupDate(when: Date): string {
  return `${when.getUTCDate()} ${MONTHS_LOWER[when.getUTCMonth()]} ${when.getUTCFullYear()}`;
}

/**
 * Waitlist confirmation email, built to the design handoff
 * (`design_handoff_waitlist_email`): 600px table layout, fully inlined
 * styles, no web fonts, no JS, no external images. The only image is the
 * wordmark, which rides along as an inline CID attachment (see
 * `fetchLogoAttachment`) and degrades to the type-only `coral·swarm`
 * wordmark when the asset can't be fetched — so the brand name is never
 * only in an image.
 *
 * The handoff's CTA button ("Read how it actually works") is deliberately
 * dropped — the email now has exactly one link, the unsubscribe, which is
 * what the copy promises ("nothing in between") and what keeps this
 * unambiguously transactional rather than promotional.
 *
 * House rules encoded here, in the handoff's words:
 *  - coral #F77764 appears exactly three times (wordmark middot, the one
 *    italic phrase in the title, the 01/02/03 numerals);
 *  - no shadow — the 1px border carries the separation, because clients
 *    render box-shadow inconsistently.
 *
 * Outlook needs explicit widths on every table/cell,
 * `mso-line-height-rule:exactly` with px line-heights, and the
 * `<!--[if mso]>` font override. Several clients drop `<style>` entirely, so
 * the inline styles alone must read correctly — the media query is an
 * enhancement only.
 */
function confirmationEmailHtml(unsubscribeUrl: string, signupDate: string, hasLogo: boolean): string {
  const wordmark = hasLogo
    ? `<img src="cid:${LOGO_CONTENT_ID}" width="200" height="26" alt="CoralSwarm" style="display:block;border:0;outline:none;text-decoration:none;">`
    : `coral<span style="color:#F77764;">&middot;</span>swarm`;

  const step = (num: string, copy: string, first: boolean) => {
    const pad = first ? "" : "padding-top:12px;";
    const rule = first
      ? ""
      : `<tr><td colspan="2" style="border-top:1px solid #EDEFF2;font-size:0;line-height:0;padding-top:12px;">&nbsp;</td></tr>`;
    return `${rule}
                <tr>
                  <td width="26" valign="top" style="width:26px;font-family:'Courier New',Courier,monospace;font-size:12px;color:#F77764;mso-line-height-rule:exactly;line-height:26px;${pad}">${num}</td>
                  <td valign="top" style="font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#14161A;mso-line-height-rule:exactly;line-height:26px;${pad}">${copy}</td>
                </tr>`;
  };

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="light dark">
<meta name="supported-color-schemes" content="light dark">
<title>You&rsquo;re on the list &mdash; CoralSwarm</title>
<!--[if mso]>
<style>body,table,td,a{font-family:Arial,Helvetica,sans-serif !important}</style>
<![endif]-->
<style>
  @media only screen and (max-width:620px){
    .cs-pad{padding-left:20px !important;padding-right:20px !important}
    .cs-title{font-size:30px !important;line-height:1.15 !important}
    .cs-stack{display:block !important;width:100% !important}
  }
</style>
</head>
<body style="margin:0;padding:0;background-color:#F5F6F8;-webkit-text-size-adjust:100%;">
<span style="display:none;font-size:1px;color:#F5F6F8;line-height:1px;max-height:0;max-width:0;opacity:0;overflow:hidden;">You&rsquo;re on the waitlist. One invite email when it&rsquo;s your turn &mdash; nothing in between.</span>

<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="background-color:#F5F6F8;">
<tr>
<td align="center" style="padding:32px 12px 48px 12px;">

  <!-- The handoff shipped this wrapper as width:600px;max-width:600px, which
       caps the width but can't force it below 600px — the email overflowed
       horizontally on every phone. (It declares a .cs-stack class for exactly
       this and never applies it to any element.) width:100% + max-width:600px
       is the canonical fix: modern clients fluid up to 600px, and Outlook,
       which reads the width attribute rather than the CSS, still gets 600.
       Fixed inline rather than via the media query on purpose — several
       clients drop <style> entirely. -->
  <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="600" style="width:100%;max-width:600px;">

    <!-- wordmark -->
    <tr>
      <td class="cs-pad" style="padding:0 8px 18px 8px;font-family:Arial,Helvetica,sans-serif;font-size:15px;font-weight:bold;letter-spacing:-0.6px;color:#14161A;mso-line-height-rule:exactly;line-height:20px;">
        ${wordmark}
      </td>
    </tr>

    <!-- card -->
    <tr>
      <td style="background-color:#FFFFFF;border:1px solid #E4E6EA;border-radius:12px;">
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="width:100%;">
          <tr>
            <td class="cs-pad" style="padding:36px 40px 8px 40px;font-family:'Courier New',Courier,monospace;font-size:11px;letter-spacing:1.4px;text-transform:uppercase;color:#8A9099;mso-line-height-rule:exactly;line-height:16px;">
              waitlist &middot; confirmed
            </td>
          </tr>
          <tr>
            <td class="cs-pad cs-title" style="padding:6px 40px 0 40px;font-family:Georgia,'Times New Roman',serif;font-size:36px;font-weight:normal;letter-spacing:-0.4px;color:#14161A;mso-line-height-rule:exactly;line-height:42px;">
              You&rsquo;re <em style="color:#F77764;">on the list</em>.
            </td>
          </tr>
          <tr>
            <td class="cs-pad" style="padding:18px 40px 0 40px;font-family:Arial,Helvetica,sans-serif;font-size:15px;color:#4A4F57;mso-line-height-rule:exactly;line-height:26px;">
              Thanks for signing up. CoralSwarm is small and invites go out in batches, so there will be a wait &mdash; probably a few weeks, not days. Nothing is running on your account yet.
            </td>
          </tr>

          <tr><td class="cs-pad" style="padding:28px 40px 0 40px;"><table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="width:100%;"><tr><td style="border-top:1px solid #EDEFF2;font-size:0;line-height:0;">&nbsp;</td></tr></table></td></tr>

          <tr>
            <td class="cs-pad" style="padding:22px 40px 0 40px;font-family:'Courier New',Courier,monospace;font-size:11px;letter-spacing:1.4px;text-transform:uppercase;color:#8A9099;mso-line-height-rule:exactly;line-height:16px;">
              what happens next
            </td>
          </tr>
          <tr>
            <td class="cs-pad" style="padding:12px 40px 36px 40px;">
              <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="width:100%;">
                ${step("01", "One email when it&rsquo;s your turn, with a link that sets up your workspace.", true)}
                ${step("02", "You connect a source &mdash; a Slack channel, a transcript, a pasted note. It forms topics; you confirm them.", false)}
                ${step("03", "Nothing in between. No drip sequence, no product newsletter, no &ldquo;still thinking about us?&rdquo;", false)}
              </table>
            </td>
          </tr>
        </table>
      </td>
    </tr>

    <!-- quiet note -->
    <tr>
      <td style="padding:14px 0 0 0;">
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="width:100%;background-color:#FAFBFC;border:1px solid #EDEFF2;border-radius:12px;">
          <tr>
            <td class="cs-pad" style="padding:20px 24px;font-family:Arial,Helvetica,sans-serif;font-size:13.5px;color:#4A4F57;mso-line-height-rule:exactly;line-height:24px;">
              One thing worth knowing early: your workspace is encrypted with a recovery key only you hold. We can&rsquo;t read it &mdash; which also means we can&rsquo;t recover it for you. The setup screen will make you save it before anything else.
            </td>
          </tr>
        </table>
      </td>
    </tr>

    <!-- footer -->
    <tr>
      <td class="cs-pad" style="padding:26px 8px 0 8px;font-family:'Courier New',Courier,monospace;font-size:11px;color:#8A9099;mso-line-height-rule:exactly;line-height:20px;">
        you joined the waitlist at coralswarm.example &middot; ${signupDate}<br>
        <a href="${unsubscribeUrl}" style="color:#8A9099;text-decoration:underline;">leave the waitlist</a> &mdash; it takes one click and removes your address.${POSTAL_ADDRESS ? `<br>\n        ${POSTAL_ADDRESS}` : ""}
      </td>
    </tr>

  </table>

</td>
</tr>
</table>
</body>
</html>`;
}

function confirmationEmailText(unsubscribeUrl: string, signupDate: string): string {
  return [
    "You're on the list.",
    "",
    "Thanks for signing up. CoralSwarm is small and invites go out in batches,",
    "so there will be a wait — probably a few weeks, not days. Nothing is",
    "running on your account yet.",
    "",
    "WHAT HAPPENS NEXT",
    "",
    "01  One email when it's your turn, with a link that sets up your workspace.",
    "02  You connect a source — a Slack channel, a transcript, a pasted note.",
    "    It forms topics; you confirm them.",
    "03  Nothing in between. No drip sequence, no product newsletter, no",
    '    "still thinking about us?"',
    "",
    "One thing worth knowing early: your workspace is encrypted with a recovery",
    "key only you hold. We can't read it — which also means we can't recover it",
    "for you. The setup screen will make you save it before anything else.",
    "",
    "—",
    `you joined the waitlist at coralswarm.example · ${signupDate}`,
    `leave the waitlist — it takes one click and removes your address: ${unsubscribeUrl}`,
    ...(POSTAL_ADDRESS ? [POSTAL_ADDRESS] : []),
  ].join("\n");
}

async function sendConfirmationEmail(email: string, env: Env): Promise<void> {
  const origin = "https://coralswarm.example";
  const token = await computeUnsubscribeToken(email, env.UNSUBSCRIBE_SECRET);
  // `encodeURIComponent` percent-encodes every character that is syntactically
  // meaningful in an href (including `&`, `"`, `<`), and the token is hex, so
  // this URL is safe to interpolate into both the HTML attribute and the
  // List-Unsubscribe header without further escaping.
  const unsubscribeUrl = `${origin}/api/waitlist/unsubscribe?email=${encodeURIComponent(email)}&token=${token}`;
  const signupDate = formatSignupDate(new Date());
  const logo = await fetchLogoAttachment(env);

  await env.EMAIL.send({
    to: email,
    from: { email: "person@example.com", name: "CoralSwarm" },
    subject: "You're on the CoralSwarm waitlist",
    text: confirmationEmailText(unsubscribeUrl, signupDate),
    html: confirmationEmailHtml(unsubscribeUrl, signupDate, logo !== null),
    headers: {
      "List-Unsubscribe": `<${unsubscribeUrl}>`,
      "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
    },
    ...(logo ? { attachments: [logo] } : {}),
  });
}

/**
 * Tell the founder a submission came in. This is what makes the page's
 * "lands in my inbox at the same time" and "a reply within one working day"
 * true rather than aspirational.
 *
 * Deliberately NO Reply-To pointing at the submitter. The address is
 * unverified at this point — anyone can type anyone else's — so setting
 * Reply-To would let a submitter choose where a reply is delivered. The
 * address is in the body instead, to be copied consciously.
 */
async function sendFounderNotification(
  submission: {
    email: string;
    name: string | null;
    scope: string;
    message: string | null;
    country: string | null;
    utmSource: string | null;
    referrer: string | null;
  },
  env: Env
): Promise<void> {
  if (!env.CONTACT_NOTIFY_TO) {
    console.error("CONTACT_NOTIFY_TO is not set — submission stored but nobody was notified");
    return;
  }

  const name = submission.name ?? "(no name)";
  const message = submission.message ?? "(no message)";
  const provenance = [
    submission.country ? `country: ${submission.country}` : null,
    submission.utmSource ? `utm_source: ${submission.utmSource}` : null,
    submission.referrer ? `referrer: ${submission.referrer}` : null,
  ]
    .filter(Boolean)
    .join(" · ");

  await env.EMAIL.send({
    to: env.CONTACT_NOTIFY_TO,
    from: { email: "person@example.com", name: "CoralSwarm site" },
    subject: `[${submission.scope}] ${name} — ${submission.email}`,
    text: [
      `scope:   ${submission.scope}`,
      `name:    ${name}`,
      `email:   ${submission.email}`,
      provenance ? `source:  ${provenance}` : null,
      "",
      message,
    ]
      .filter((l) => l !== null)
      .join("\n"),
    html: `
      <p style="font:13px/1.6 -apple-system,system-ui,sans-serif;color:#4b5058;margin:0 0 14px 0">
        <strong style="color:#14161a">${escapeHtml(name)}</strong> — ${escapeHtml(submission.email)}<br>
        scope: ${escapeHtml(submission.scope)}${provenance ? `<br>${escapeHtml(provenance)}` : ""}
      </p>
      <p style="font:14px/1.7 -apple-system,system-ui,sans-serif;color:#14161a;white-space:pre-wrap;margin:0">${escapeHtml(message)}</p>
    `.trim(),
  });
}

async function handleContact(request: Request, env: Env): Promise<Response> {
  // Per-IP throttle (5 req/60s, see wrangler.jsonc). Turnstile is the real
  // bot control; this caps the damage a single client can do while holding
  // a valid token, and keeps D1 write costs bounded.
  const clientIp = request.headers.get("CF-Connecting-IP") ?? "unknown";
  const { success } = await env.WAITLIST_RATE_LIMITER.limit({ key: clientIp });
  if (!success) {
    return json(
      { ok: false, error: "Too many requests. Please try again in a minute." },
      { status: 429 }
    );
  }

  let body: Record<string, unknown> = {};
  try {
    const payload = await request.json();
    // `?? {}` guards a literal JSON `null` body (valid JSON, would
    // otherwise throw on property access below).
    body = (payload as Record<string, unknown>) ?? {};
  } catch {
    return json({ ok: false, error: "Invalid request body." }, { status: 400 });
  }

  // Verify BEFORE touching the database or sending anything.
  const verdict = await verifyTurnstile(body.turnstile_token, clientIp, env);
  if (!verdict.ok) {
    return json({ ok: false, error: verdict.error }, { status: verdict.status });
  }

  const rawEmail = body.email;
  if (typeof rawEmail !== "string") {
    return json({ ok: false, error: "An email address is required." }, { status: 400 });
  }
  const email = rawEmail.trim().toLowerCase();
  if (email.length === 0 || email.length > MAX_EMAIL_LEN || !EMAIL_RE.test(email)) {
    return json({ ok: false, error: "Please enter a valid email address." }, { status: 400 });
  }

  const name = sanitizeText(body.name, MAX_NAME_LEN);
  if (!name) {
    return json({ ok: false, error: "Please enter your name." }, { status: 400 });
  }

  // Closed set, mirroring the form's two-button toggle and the CHECK
  // constraint on contact_submissions.
  const scope = body.scope === "enterprise" ? "enterprise" : "personal";
  const message = sanitizeText(body.message, MAX_MESSAGE_LEN);

  // First-touch attribution — optional, client-supplied, sanitized. Country
  // is the one exception: derived server-side from Cloudflare's edge, never
  // taken from the client.
  const utmSource = sanitizeAttributionField(body.utm_source);
  const utmMedium = sanitizeAttributionField(body.utm_medium);
  const utmCampaign = sanitizeAttributionField(body.utm_campaign);
  const referrer = sanitizeAttributionField(body.referrer);
  const landingPath = sanitizeAttributionField(body.landing_path);
  const firstSeenAt = sanitizeAttributionField(body.first_seen_at);
  const country = sanitizeCountry(request.headers.get("CF-IPCountry"));

  let isNewSignup = false;
  try {
    // Two writes, batched so a submission is all-or-nothing:
    //
    //  1. waitlist_signups — deduped. ON CONFLICT DO NOTHING keeps a
    //     resubmission a success from the caller's point of view (they're
    //     already on the list) and preserves first-touch semantics.
    //     meta.changes distinguishes a real insert (1) from a no-op (0),
    //     so a duplicate doesn't re-send the confirmation email.
    //  2. contact_submissions — append-only. ALWAYS inserted, so a
    //     returning lead's message is never the thing that gets dropped.
    const [signup] = await env.DB.batch([
      env.DB.prepare(
        `INSERT INTO waitlist_signups (
           email, name, scope, message,
           utm_source, utm_medium, utm_campaign, referrer, landing_path, country, first_seen_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
         ON CONFLICT(email) DO NOTHING`
      ).bind(
        email,
        name,
        scope,
        message,
        utmSource,
        utmMedium,
        utmCampaign,
        referrer,
        landingPath,
        country,
        firstSeenAt
      ),
      env.DB.prepare(
        `INSERT INTO contact_submissions (
           email, name, scope, message,
           utm_source, utm_medium, utm_campaign, referrer, landing_path, country, first_seen_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)`
      ).bind(
        email,
        name,
        scope,
        message,
        utmSource,
        utmMedium,
        utmCampaign,
        referrer,
        landingPath,
        country,
        firstSeenAt
      ),
    ]);
    isNewSignup = signup.meta.changes > 0;
  } catch (err) {
    console.error("contact insert failed", err);
    return json({ ok: false, error: "Something went wrong. Please try again." }, { status: 500 });
  }

  // Both sends are best-effort: the submission is already committed, so an
  // email failure must not turn a real signup into an error response.
  try {
    await sendFounderNotification(
      { email, name, scope, message, country, utmSource, referrer },
      env
    );
  } catch (err) {
    console.error("founder notification failed", err);
  }

  if (isNewSignup) {
    try {
      await sendConfirmationEmail(email, env);
    } catch (err) {
      console.error("confirmation email failed", err);
    }
  }

  return json({ ok: true });
}

async function handleUnsubscribe(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  const email = url.searchParams.get("email")?.trim().toLowerCase() ?? "";
  const token = url.searchParams.get("token") ?? "";

  const expected = email ? await computeUnsubscribeToken(email, env.UNSUBSCRIBE_SECRET) : "";
  // Both sides are fixed-length hex from the same HMAC function, so a plain
  // === compare is fine here — this isn't a session-auth-shaped timing risk.
  if (!email || !token || token !== expected) {
    return html("<p>That unsubscribe link isn't valid.</p>", { status: 400 });
  }

  try {
    await env.DB.prepare("DELETE FROM waitlist_signups WHERE email = ?1").bind(email).run();
  } catch (err) {
    console.error("unsubscribe failed", err);
    return html("<p>Something went wrong. Please try again.</p>", { status: 500 });
  }

  // RFC 8058 one-click: mail clients POST here on the recipient's behalf
  // with no UI of their own — a bare 200 is the whole contract, no page
  // needed. A human clicking the link in the email body arrives via GET
  // and gets a real confirmation page.
  if (request.method === "POST") {
    return new Response(null, { status: 200 });
  }
  return html("<p>You've been removed from the CoralSwarm waitlist.</p>");
}

// Best-effort bot filter for the pageview log — NOT real bot detection (no
// JS-challenge, no header-consistency checks, nothing that would survive a
// motivated scraper). It exists only to keep the obvious, honest crawlers
// (search engines, uptime/preview-unfurl bots, headless test runners) out
// of a "how many humans looked at this page" number, at the cost of a
// single case-insensitive substring test.
const BOT_UA_RE = /bot|crawler|spider|preview|headless/i;

/**
 * First-party, cookieless pageview log — POST /api/waves.
 *
 * Named to survive ad-block filter lists: EasyPrivacy and friends match on
 * path substrings like /track, /collect, /analytics, /beacon, /pv, /stats,
 * /event (and this Worker already ships one of those — see handleContact's
 * doc comment about the old /api/waitlist route, unrelated history but same
 * naming lesson). "waves" describes what the site actually calls its own
 * visitors' activity elsewhere in the copy and matches none of the known
 * patterns.
 *
 * Mirrors handleContact's hygiene end to end: every field is
 * attacker-controlled (unauthenticated POST body) and sanitized/capped
 * before touching D1; country is derived server-side from CF-IPCountry,
 * never taken from the body. No CORS headers are sent, but that alone
 * doesn't make this same-origin: missing CORS headers only stop a browser
 * from *reading* the response, not from *sending* the write (a
 * CORS-safelisted `Content-Type: text/plain` body skips preflight
 * entirely, and a bare curl loop needs no browser at all). The explicit
 * Origin check below is what actually keeps another site from posting
 * forged attribution rows into this table.
 */
async function handlePageView(request: Request, env: Env): Promise<Response> {
  // VisitPing (components/VisitPing.tsx) always posts from the site
  // itself, so a same-origin Origin header is present for every legitimate
  // request in a modern browser. Reject anything else before it reaches
  // request.json() / D1.
  const origin = request.headers.get("Origin");
  if (origin !== null && origin !== new URL(request.url).origin) {
    return new Response(null, { status: 403 });
  }

  const clientIp = request.headers.get("CF-Connecting-IP") ?? "unknown";

  // Dedicated PAGEVIEW_RATE_LIMITER (30 req/60s, see wrangler.jsonc) rather
  // than reusing WAITLIST_RATE_LIMITER — a pageview ping fires once per tab
  // session on every page load, so it wants a much looser cap than a form a
  // visitor submits once. It's a separate namespace_id from
  // WAITLIST_RATE_LIMITER, so the key doesn't need a `pv:` prefix to avoid
  // colliding with a visitor's own contact-form submission bucket.
  const { success } = await env.PAGEVIEW_RATE_LIMITER.limit({ key: clientIp });
  if (!success) {
    return new Response(null, { status: 429 });
  }

  const userAgent = request.headers.get("User-Agent") ?? "";
  if (BOT_UA_RE.test(userAgent)) {
    // Quietly accepted, nothing written — see BOT_UA_RE. The caller is
    // fire-and-forget and doesn't inspect the response either way.
    return new Response(null, { status: 204 });
  }

  let body: Record<string, unknown> = {};
  try {
    const payload = await request.json();
    body = (payload as Record<string, unknown>) ?? {};
  } catch {
    return new Response(null, { status: 400 });
  }

  // Same sanitizer as first-touch attribution — trims, drops non-strings,
  // caps length. `path` gets it too: it's client-supplied
  // (window.location.pathname) just like the others, and the cap is
  // generous enough for any real route on this site.
  const path = sanitizeAttributionField(body.path);
  const utmSource = sanitizeAttributionField(body.utm_source);
  const utmMedium = sanitizeAttributionField(body.utm_medium);
  const utmCampaign = sanitizeAttributionField(body.utm_campaign);
  const referrer = sanitizeAttributionField(body.referrer);
  const country = sanitizeCountry(request.headers.get("CF-IPCountry"));
  const isFirstVisit = body.is_first_visit === true ? 1 : 0;

  try {
    await env.DB.prepare(
      `INSERT INTO page_views (
         path, utm_source, utm_medium, utm_campaign, referrer, country, is_first_visit
       ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)`
    )
      .bind(path, utmSource, utmMedium, utmCampaign, referrer, country, isFirstVisit)
      .run();
  } catch (err) {
    // A dropped log row isn't something the visitor should ever see or the
    // client should retry — best-effort logging, same posture as the email
    // sends in handleContact.
    console.error("page view insert failed", err);
  }

  return new Response(null, { status: 204 });
}

// ---------------------------------------------------------------------------
// GET /api/seo-stats — token-gated stats for the SEO watchdog agent
// ---------------------------------------------------------------------------
//
// Combines first-party pageview counts (D1, this Worker's own table) with
// Google Search Console's Search Analytics API (indexing/query performance
// this Worker has no other visibility into) into one JSON payload an
// external agent polls every ~6h. Two independent secrets gate/empower it:
//
//   SEO_STATS_TOKEN         — bearer token the CALLER presents. Set with
//                              `wrangler secret put SEO_STATS_TOKEN` from
//                              marketing-web/.
//   GSC_SERVICE_ACCOUNT_KEY — the full Google service-account JSON THIS
//                              WORKER presents to Google. Set with
//                              `wrangler secret put GSC_SERVICE_ACCOUNT_KEY`
//                              from marketing-web/ (paste the whole
//                              downloaded JSON file as the value). The
//                              account must be added as a verified user of
//                              the `sc-domain:coralswarm.example` property in
//                              Search Console — this Worker can't do that
//                              part, it's a one-time console step.
//
// The Search Console block degrades to `{available: false, reason}` on any
// failure (bad/missing credential, Google error, no data yet) rather than
// failing the request — the traffic block, which needs neither secret, must
// always be served.

interface TrafficByPath {
  path: string;
  last24h: number;
  last7d: number;
}

interface TrafficDaily {
  date: string;
  count: number;
}

interface TrafficBlock {
  last24h_total: number;
  last7d_total: number;
  by_path: TrafficByPath[];
  daily: TrafficDaily[];
}

/** Row shapes as they come back from D1 (`batch` results are untyped). */
interface CountRow {
  count: number;
}
interface ByPathRow {
  path: string | null;
  last24h: number;
  last7d: number;
}
interface DailyCountRow {
  date: string;
  count: number;
}

/**
 * All four queries in one `batch()` call, each a single aggregation (never
 * one row per pageview) so the response stays cheap regardless of table
 * size. `by_path` uses conditional aggregation (`SUM(CASE WHEN ...)`) to get
 * both windows' counts per path out of one GROUP BY rather than two queries
 * that would need joining in JS.
 */
async function fetchTraffic(env: Env): Promise<TrafficBlock> {
  const [totalDay, totalWeek, byPath, daily] = await env.DB.batch<
    CountRow | ByPathRow | DailyCountRow
  >([
    env.DB.prepare(
      `SELECT COUNT(*) AS count FROM page_views WHERE created_at >= datetime('now', '-1 day')`
    ),
    env.DB.prepare(
      `SELECT COUNT(*) AS count FROM page_views WHERE created_at >= datetime('now', '-7 days')`
    ),
    env.DB.prepare(
      `SELECT
         path,
         SUM(CASE WHEN created_at >= datetime('now', '-1 day') THEN 1 ELSE 0 END) AS last24h,
         SUM(CASE WHEN created_at >= datetime('now', '-7 days') THEN 1 ELSE 0 END) AS last7d
       FROM page_views
       WHERE created_at >= datetime('now', '-7 days')
       GROUP BY path
       ORDER BY last7d DESC
       LIMIT 20`
    ),
    env.DB.prepare(
      `SELECT date(created_at) AS date, COUNT(*) AS count
       FROM page_views
       WHERE created_at >= datetime('now', '-7 days')
       GROUP BY date(created_at)
       ORDER BY date ASC`
    ),
  ]);

  const pathRows = (byPath.results ?? []) as ByPathRow[];
  const dailyRows = (daily.results ?? []) as DailyCountRow[];

  return {
    last24h_total: Number((totalDay.results?.[0] as CountRow | undefined)?.count ?? 0),
    last7d_total: Number((totalWeek.results?.[0] as CountRow | undefined)?.count ?? 0),
    by_path: pathRows.map((r) => ({
      path: r.path ?? "(unknown)",
      last24h: Number(r.last24h),
      last7d: Number(r.last7d),
    })),
    daily: dailyRows.map((r) => ({ date: r.date, count: Number(r.count) })),
  };
}

interface GscMetrics {
  clicks: number;
  impressions: number;
  ctr: number;
  position: number;
}
interface GscQueryRow extends GscMetrics {
  query: string;
}
interface GscPageRow extends GscMetrics {
  page: string;
}
interface GscDailyRow extends GscMetrics {
  date: string;
}
type SearchConsoleBlock =
  | {
      available: true;
      window: { start: string; end: string };
      totals: GscMetrics;
      top_queries: GscQueryRow[];
      top_pages: GscPageRow[];
      daily: GscDailyRow[];
    }
  | { available: false; reason: string };

function addUTCDays(date: Date, days: number): Date {
  const copy = new Date(date);
  copy.setUTCDate(copy.getUTCDate() + days);
  return copy;
}

function isoDateOnly(date: Date): string {
  return date.toISOString().slice(0, 10);
}

function base64UrlFromBytes(bytes: Uint8Array): string {
  let binary = "";
  for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
  // btoa's output is always plain base64 (ASCII in, ASCII out) — the
  // URL-safe substitutions below are a fixed-width string transform, no
  // dynamic RegExp on attacker input, so this is safe by construction.
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function base64UrlFromString(value: string): string {
  return base64UrlFromBytes(new TextEncoder().encode(value));
}

const PKCS8_HEADER_RE = /-----BEGIN PRIVATE KEY-----/;
const PKCS8_FOOTER_RE = /-----END PRIVATE KEY-----/;

/**
 * Import the PEM-encoded PKCS#8 private key out of a Google service-account
 * JSON blob. Per the task spec: strip the PEM header/footer, base64-decode
 * the remainder, hand the raw DER bytes to WebCrypto's `pkcs8` importer.
 */
async function importGoogleServiceAccountKey(pem: string): Promise<CryptoKey> {
  const base64 = pem.replace(PKCS8_HEADER_RE, "").replace(PKCS8_FOOTER_RE, "").replace(/\s+/g, "");
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return crypto.subtle.importKey(
    "pkcs8",
    bytes.buffer,
    { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
    false,
    ["sign"]
  );
}

/**
 * Mint a short-lived (1h) self-signed JWT asserting the service account's
 * identity and the readonly Search Console scope, per Google's
 * server-to-server OAuth flow (no user in the loop — that's what makes a
 * bare service-account key usable from a Worker with no stored refresh
 * token).
 */
async function signGoogleServiceAccountJwt(clientEmail: string, privateKeyPem: string): Promise<string> {
  const nowSeconds = Math.floor(Date.now() / 1000);
  const header = { alg: "RS256", typ: "JWT" };
  const claims = {
    iss: clientEmail,
    scope: "https://www.googleapis.com/auth/webmasters.readonly",
    aud: "https://oauth2.googleapis.com/token",
    exp: nowSeconds + 3600,
    iat: nowSeconds,
  };
  const unsigned = `${base64UrlFromString(JSON.stringify(header))}.${base64UrlFromString(
    JSON.stringify(claims)
  )}`;
  const key = await importGoogleServiceAccountKey(privateKeyPem);
  const signature = await crypto.subtle.sign(
    "RSASSA-PKCS1-v1_5",
    key,
    new TextEncoder().encode(unsigned)
  );
  return `${unsigned}.${base64UrlFromBytes(new Uint8Array(signature))}`;
}

/** Exchange the signed JWT for a bearer access token. Returns null on any failure. */
async function fetchGoogleAccessToken(clientEmail: string, privateKeyPem: string): Promise<string | null> {
  const assertion = await signGoogleServiceAccountJwt(clientEmail, privateKeyPem);
  const resp = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
      assertion,
    }).toString(),
    // Never let a stalled/hanging Google call block the whole request — this
    // is a best-effort block that degrades to `available: false` on any
    // failure, so a 10s cap on each outbound leg is far better than the
    // Worker itself timing out.
    signal: AbortSignal.timeout(10_000),
  });
  if (!resp.ok) {
    console.error("GSC token exchange failed", resp.status);
    return null;
  }
  const data = (await resp.json()) as { access_token?: string };
  return data.access_token ?? null;
}

const GSC_PROPERTY = "sc-domain:coralswarm.example";
const GSC_QUERY_URL = `https://www.googleapis.com/webmasters/v3/sites/${encodeURIComponent(
  GSC_PROPERTY
)}/searchAnalytics/query`;

interface GscApiRow {
  keys?: string[];
  clicks?: number;
  impressions?: number;
  ctr?: number;
  position?: number;
}

/** One searchAnalytics/query call. Returns null (not []) on any HTTP-level failure. */
async function queryGscRows(
  accessToken: string,
  window: { start: string; end: string },
  dimensions: string[],
  rowLimit?: number
): Promise<GscApiRow[] | null> {
  const resp = await fetch(GSC_QUERY_URL, {
    method: "POST",
    headers: {
      authorization: `Bearer ${accessToken}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({
      startDate: window.start,
      endDate: window.end,
      dimensions,
      ...(rowLimit !== undefined ? { rowLimit } : {}),
    }),
    // Same reasoning as the token exchange above — bound every outbound leg
    // to Google individually.
    signal: AbortSignal.timeout(10_000),
  });
  if (!resp.ok) {
    console.error("GSC searchAnalytics/query failed", resp.status, dimensions);
    return null;
  }
  const data = (await resp.json()) as { rows?: GscApiRow[] };
  return data.rows ?? [];
}

function gscMetrics(row: GscApiRow | undefined): GscMetrics {
  return {
    clicks: row?.clicks ?? 0,
    impressions: row?.impressions ?? 0,
    ctr: row?.ctr ?? 0,
    position: row?.position ?? 0,
  };
}

/**
 * Build the Search Console block, never throwing — every failure mode
 * (unset/malformed secret, token exchange failure, API error, a
 * brand-new property with no data at all yet) collapses to
 * `{available: false, reason}` so a GSC hiccup never takes the whole
 * endpoint down with it.
 */
async function fetchSearchConsole(env: Env): Promise<SearchConsoleBlock> {
  try {
    if (!env.GSC_SERVICE_ACCOUNT_KEY) {
      return { available: false, reason: "GSC_SERVICE_ACCOUNT_KEY is not set" };
    }

    let parsed: unknown;
    try {
      parsed = JSON.parse(env.GSC_SERVICE_ACCOUNT_KEY);
    } catch {
      // Never echo the raw secret value into logs or the response.
      return { available: false, reason: "GSC_SERVICE_ACCOUNT_KEY is not valid JSON" };
    }

    // `JSON.parse` returns `any` — treat it as `unknown` and narrow by hand
    // rather than trusting the shape, since this is attacker-adjacent
    // (whatever got pasted into the secret) parsed data feeding straight
    // into a crypto import.
    const record = typeof parsed === "object" && parsed !== null ? (parsed as Record<string, unknown>) : null;
    const clientEmail = record?.client_email;
    const privateKey = record?.private_key;
    if (typeof clientEmail !== "string" || clientEmail.length === 0 || typeof privateKey !== "string" || privateKey.length === 0) {
      return {
        available: false,
        reason: "GSC_SERVICE_ACCOUNT_KEY missing client_email/private_key",
      };
    }
    const credentials: { client_email: string; private_key: string } = {
      client_email: clientEmail,
      private_key: privateKey,
    };

    const accessToken = await fetchGoogleAccessToken(credentials.client_email, credentials.private_key);
    if (!accessToken) {
      return { available: false, reason: "failed to obtain a Google access token" };
    }

    // GSC data lags roughly 2 days behind real time, so the window's end is
    // today-2 rather than today — querying up through "today" would just
    // return a truncated/empty final day every single call.
    const end = addUTCDays(new Date(), -2);
    const start = addUTCDays(end, -27);
    const window = { start: isoDateOnly(start), end: isoDateOnly(end) };

    const [totalsRows, queryRows, pageRows, dailyRows] = await Promise.all([
      queryGscRows(accessToken, window, []),
      queryGscRows(accessToken, window, ["query"], 25),
      queryGscRows(accessToken, window, ["page"], 25),
      queryGscRows(accessToken, window, ["date"]),
    ]);

    if (totalsRows === null || queryRows === null || pageRows === null || dailyRows === null) {
      return { available: false, reason: "Search Console API request failed" };
    }
    if (totalsRows.length === 0) {
      // A verified-but-brand-new (or effectively unindexed) property
      // returns a fully empty result set rather than an error — treat that
      // the same as "not available yet" instead of shipping an all-zero
      // block that reads as a real (and alarming) traffic collapse.
      return { available: false, reason: "no Search Console data yet for this property/window" };
    }

    return {
      available: true,
      window,
      totals: gscMetrics(totalsRows[0]),
      top_queries: queryRows.map((r) => ({ query: r.keys?.[0] ?? "", ...gscMetrics(r) })),
      top_pages: pageRows.map((r) => ({ page: r.keys?.[0] ?? "", ...gscMetrics(r) })),
      daily: dailyRows.map((r) => ({ date: r.keys?.[0] ?? "", ...gscMetrics(r) })),
    };
  } catch (err) {
    console.error("search console block threw", err);
    return { available: false, reason: "unexpected error building the Search Console block" };
  }
}

/**
 * GET /api/seo-stats — see the file-level comment block above for the two
 * secrets this depends on.
 *
 * Auth-then-cache, strictly in that order: the bearer check runs first, and
 * only a request that already proved the token gets to consult (or
 * populate) the shared edge cache. `caches.default` keys on origin +
 * pathname only — no query string, no Authorization header, no Set-Cookie in
 * the response — so the cache itself carries no auth state and can't be
 * multiplied by a `?cachebust=`-style query param. The ordering here is what
 * actually prevents an unauthenticated `GET /api/seo-stats` from ever being
 * served a cached hit; the response returned to the caller always carries
 * `Cache-Control: no-store` regardless of hit or miss, so only this Worker's
 * own internal cache — never a browser or intermediate proxy — is allowed to
 * retain a copy.
 */
async function handleSeoStats(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
  if (request.method !== "GET") {
    return json({ ok: false, error: "Method not allowed." }, { status: 405 });
  }

  const authHeader = request.headers.get("Authorization") ?? "";
  // Exact match against a long-lived, agent-held bearer token — not a
  // session credential fronting a login flow, so the task's own spec calls
  // out that constant-time comparison isn't required here.
  if (!env.SEO_STATS_TOKEN || authHeader !== `Bearer ${env.SEO_STATS_TOKEN}`) {
    return json({ ok: false, error: "Unauthorized." }, { status: 401 });
  }

  const cache = caches.default;
  const url = new URL(request.url);
  // Cache key is origin + pathname ONLY — deliberately dropping the query
  // string (and not the incoming `request` object at all, so the key can
  // never vary on or leak the Authorization header above). This route takes
  // no query params today; if it ever did, keying on them too would let
  // `?cachebust=1`-style requests multiply cache entries and burn the
  // quota-limited GSC service account on every miss.
  const cacheKey = new Request(`${url.origin}${url.pathname}`, { method: "GET" });
  const cached = await cache.match(cacheKey);
  if (cached) {
    // The cached Response's headers are immutable (Cloudflare's cache guard)
    // — build a fresh Response around the same body to get a mutable copy
    // before overriding the header below.
    const outgoing = new Response(cached.body, cached);
    // This response is only ever reachable after the bearer check above, so
    // it must never be cacheable by anything downstream of this Worker
    // (shared proxies, the requester's own HTTP client) — only the internal
    // caches.default copy (see below) is allowed to persist it.
    outgoing.headers.set("cache-control", "no-store");
    return outgoing;
  }

  const [traffic, searchConsole] = await Promise.all([fetchTraffic(env), fetchSearchConsole(env)]);

  // This is the copy that goes into caches.default — plain 1h max-age, no
  // `public`/`private`/`no-store`, which is what makes cache.put() actually
  // store it. The client-facing "no-store" override happens below, on a
  // fresh Response built from a clone of this one, never on this object
  // itself.
  const stored = json(
    {
      generated_at: new Date().toISOString(),
      traffic,
      search_console: searchConsole,
    },
    {
      headers: {
        // 1h edge cache — also caps how often a single quota-limited GSC
        // service account gets called regardless of how often the watchdog
        // (or anyone else holding the token) polls this route.
        "cache-control": "max-age=3600",
      },
    }
  );

  // clone(): the cache gets its own copy of the body stream — `stored`
  // itself is still needed below to build the outgoing response.
  // waitUntil() so the request doesn't wait on the cache write, with a
  // .catch() so a cache.put() rejection (e.g. quota, transient KV/cache
  // error) surfaces as a log line instead of an unhandled rejection.
  ctx.waitUntil(
    cache
      .put(cacheKey, stored.clone())
      .catch(() => {
        console.error("seo-stats: cache.put failed");
      })
  );

  const outgoing = new Response(stored.body, stored);
  outgoing.headers.set("cache-control", "no-store");
  return outgoing;
}

/**
 * Serve the generated share card with a correct content type.
 *
 * Next writes the OG image to `out/opengraph-image` — no file extension —
 * and the meta tags point at `/opengraph-image?<hash>`. The asset server
 * infers content types from the extension, so without this the card is
 * served as a generic download and Facebook, LinkedIn, Slack and X all
 * refuse to render it. Everything else (bytes, caching, 404s) still comes
 * from ASSETS; only the header is rewritten.
 */
async function handleOgImage(request: Request, env: Env): Promise<Response> {
  const upstream = await env.ASSETS.fetch(request);
  if (!upstream.ok) return upstream;
  const headers = new Headers(upstream.headers);
  headers.set("content-type", "image/png");
  return new Response(upstream.body, { status: upstream.status, headers });
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    // www -> apex. This is the whole fix — Workers Static Assets' _redirects
    // file only matches relative-path sources, not hostnames, so a
    // www.coralswarm.example rule there is silently ignored (see
    // https://developers.cloudflare.com/workers/static-assets/redirects/).
    // run_worker_first is `true` (wrangler.jsonc) specifically so this check
    // runs for every request on both custom domains, not just /api/* and
    // /opengraph-image. 308 (not 301) so a POST — e.g. www.coralswarm.example
    // hitting /api/contact or /api/waves — replays as a POST on the apex
    // instead of being downgraded to GET.
    if (url.hostname === "www.coralswarm.example") {
      url.hostname = "coralswarm.example";
      url.protocol = "https:";
      return Response.redirect(url.toString(), 308);
    }

    if (url.pathname === "/api/contact") {
      if (request.method === "POST") {
        return handleContact(request, env);
      }
      return json({ ok: false, error: "Method not allowed." }, { status: 405 });
    }

    if (url.pathname === "/api/waitlist/unsubscribe") {
      if (request.method === "GET" || request.method === "POST") {
        return handleUnsubscribe(request, env);
      }
      return json({ ok: false, error: "Method not allowed." }, { status: 405 });
    }

    if (url.pathname === "/api/waves") {
      if (request.method === "POST") {
        return handlePageView(request, env);
      }
      return json({ ok: false, error: "Method not allowed." }, { status: 405 });
    }

    if (url.pathname === "/api/seo-stats") {
      return handleSeoStats(request, env, ctx);
    }

    if (url.pathname === "/opengraph-image") {
      return handleOgImage(request, env);
    }

    return env.ASSETS.fetch(request);
  },
};
