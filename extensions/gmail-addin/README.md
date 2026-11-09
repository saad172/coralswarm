# CoralSwarm Gmail Add-on — "Save to CoralSwarm"

The Gmail counterpart to the Outlook add-in's "Save to CoralSwarm"
(`extensions/outlook-addin`; its panes are `web/components/addin/`). One click on an open email opts its thread
into CoralSwarm — the backend does the rest (fetches the message, expands it
to the full conversation unless told to keep just the one message,
registers it for reply auto-capture unless told not to). See
`docs/connectors/google-workspace.md` for the backend side of Gmail
ingestion; `POST /v1/connectors/google/ingest`
(`backend/src/routes/connectors.rs`, `ingest_selected`) is the same route
the Outlook add-in uses, plus two optional body fields this add-on's save
options set (see "Save options and following" below).

## Why this is Apps Script + CardService, not a web bundle

The Outlook add-in hosts clerk-js directly in its task-pane iframe and mints
a fresh ~60s Clerk session JWT immediately before every call — see that
add-in's README. **That pattern does not transfer to Gmail.** A Gmail
Add-on is Google Apps Script + CardService: a server-side function returns a
declarative `Card` object fresh on every `contextualTriggers` event. There
is no persistent DOM, no iframe, and no place to keep a live Clerk session
alive. Apps Script's only durable per-user state is
`PropertiesService.getUserProperties()` — a plain key-value store — so the
add-on signs in with **Clerk OAuth** (authorization code + PKCE, public
client) and keeps the resulting refresh-able access token there. See
"Sign-in (Clerk OAuth)" below. (It used to mint its own device-paired
`csat_` token instead — `backend/src/routes/addin.rs`'s pairing design — which
is now only a migration path.)

## How it works

```
Gmail (message open, or the add-on's homepage)
   │  contextualTriggers fires onGmailMessageOpen(e) [homepageTrigger → onHomepage(e)]
   ▼
Card.gs entryCard_ → Auth.gs requireCredential_() — which credential do we hold?
   │
   ├─ none: AuthorizationException → buildSignInCard_() → "Sign in to CoralSwarm"
   │     │  tap → Clerk popup (Google sign-in, consent, workspace picker)
   │     │  → /usercallback → Auth.gs authCallback() exchanges the code
   │     │  (PKCE, no client secret), stores tokens, closes the popup;
   │     │  Gmail reloads the card
   │     ▼
   └─ has a credential (Clerk token, or a legacy csat_ token): buildConnectedCard_(e)
         │  GET /v1/connectors/google/watched/{e.gmail.threadId} → {following}
         │  (one call per open, never retried; any failure = "not following")
         │
         ├─ following=false (or unreadable): the SAVE card — "+" row, thread
         │  row, the save options (scope radio: "Whole thread · N messages"
         │  / "Just this message"; "Keep saving new replies" switch, ON),
         │  "Save to CoralSwarm" footer. The Save action carries the
         │  parameter save_options=shown.
         │     │  tap
         │     ▼
         │  Ingest.gs: saveToCoralSwarm(e)
         │     │  reads e.gmail.messageId (the OPEN message only — never the
         │     │  mailbox) and, because save_options=shown, the two knobs
         │     │  from e.formInput; POSTs
         │     │  {item_ids:[messageId], scope:"thread"|"message", follow:bool}
         │     │  to POST /v1/connectors/google/ingest with
         │     │  Authorization: Bearer <Clerk access token, or legacy csat_ token>
         │     ▼
         │  re-renders as the SAVED card (below) with the toast
         │  "Saved to CoralSwarm · N messages"
         │
         └─ following=true (or just saved / just toggled): the SAVED card —
            "In your ocean · following" row (opens Today), and the SAME
            switch, now LIVE: flipping it calls
            Ingest.gs: toggleFollow(e)
               │  PUT /v1/connectors/google/watched/{threadId} {follow:bool}
               │  → {following}; the card re-renders from the ANSWER
               │  toast "Stopped following" / "Following again"
            No scope radio here; the Save footer stays and carries the
            marker save_options=follow_only, so a re-save sends
            {item_ids:[messageId], follow:<the switch as it stands>} and
            no scope — a field is sent iff its control is rendered. The
            backend dedupes the messages ("Already in your ocean") and
            sets follow_replies to the request's value, so a thread
            toggled off stays off through a re-save. No scope means the
            backend default scope=thread: after a "Just this message"
            save, a re-save from this card ingests the REST of the thread
            — the toast shows the real count. A consequence of the rule,
            not a bug.
         ▼
      CoralSwarm backend — fetches the message, expands to the whole thread
      (scope=thread) or keeps the one message (scope=message), registers it
      for reply auto-capture (follow=true), ingests → embeds → queryable.
```

### Save options and following

The two knobs on the save card and the live switch on the saved card, and
the decisions behind how they are wired:

- **The knobs are always visible with fixed defaults** (`thread`, follow
  ON) and nothing is remembered between opens. `Card.gs`
  `saveOptionsWidgets_` builds them; the radio's count comes from the same
  `currentMessageSummary_` read the thread row uses ("Whole thread · 1
  message" when the thread has one, plain "Whole thread" when the count
  cannot be read).
- **A field is sent iff its control is rendered** — the rule the Outlook
  add-in implements; the two add-ins behave identically. Which controls a
  card rendered is carried explicitly, never inferred: an unchecked
  CardService switch is simply ABSENT from `e.formInput`, which is
  indistinguishable from "this card never had a switch". So the Save
  action (both the "+" row and the footer button) carries the action
  parameter `save_options` (`PARAM_SAVE_OPTIONS`, `Card.gs`) and
  `saveToCoralSwarm` reads it:

  | Card | Controls rendered | `save_options` | Ingest body adds |
  |---|---|---|---|
  | Save card (not following) | `scope` radio + `follow` switch | `shown` | `scope`, `follow` |
  | Saved card (following, or just saved / toggled) | live `follow` switch only | `follow_only` | `follow` (as the switch stands) |
  | (no card today) | none | absent | nothing — backend defaults |

  With a marker present, an absent `follow` input means `false`. The
  re-rendered card's "following" is what was sent (the backend sets
  `follow_replies` to the request's value on conflict), so the card is
  truthful in every state. There is no "checking" state: the on-open
  `GET` is synchronous, before anything renders, so only these two
  shapes exist.
- **Following is read once per open** — `GET …/watched/{threadId}`
  (`threadFollowing_`, `Card.gs`). Only an explicit `true` flips the card
  into its saved shape; `false` (never saved, or saved-then-unfollowed —
  the route never 404s) and every failure (network, non-2xx, malformed
  body) render the ordinary save card, so the read can never break the
  card. An unfollowed-but-saved thread therefore looks unsaved on reopen,
  by design.
- **No remove / delete action anywhere on the card.** Unfollowing is the
  only "undo", and what was saved stays.

`threadId` is `e.gmail.threadId` (the Gmail API thread id the contextual
trigger hands over under the metadata scope), with the `GmailThread.getId()`
that `currentMessageSummary_` resolves as the fallback — the same id the
backend keys `gmail_watched_threads` on.

### Manual walk (run in Gmail after the backend carrying `watched/` deploys)

Prerequisite: a backend with `GET`/`PUT /v1/connectors/google/watched/{threadId}`
and the `scope`/`follow` ingest fields is live at `BACKEND_BASE_URL`, and the
`.gs` files below are pushed to the Apps Script project (no manifest change
in this train, so no reinstall of the test deployment is needed).

1. **Fresh thread.** Open a multi-message thread never saved before. The
   card shows the "+" row, the thread row, a radio with "Whole thread · N
   messages" (selected) and "Just this message", and "Keep saving new
   replies" ON. No "In your ocean" row.
2. **Save, default knobs.** Tap Save. Toast "Saved to CoralSwarm · N
   messages"; the card re-renders as "In your ocean · following · N
   messages saved" with the switch ON and no radio. Backend: ingest body
   `{item_ids, scope:"thread", follow:true}`.
3. **Reopen** (close and reopen the message). The card opens straight in
   the saved state (GET said `true`): "In your ocean · following", switch
   ON, no radio.
4. **Toggle off.** Flip the switch. Toast "Stopped following"; the row
   drops "· following", the switch stays rendered and OFF.
5. **Reopen.** The card shows the ordinary save card again (GET said
   `false`) — knobs visible, defaults reset.
6. **Toggle on from the saved card.** The save card from step 5 is
   showing, so tap Save with its defaults: this save carries
   `save_options=shown` and sends `scope:"thread", follow:true`
   explicitly. The backend dedupes the messages (toast "Already in your
   ocean — nothing new to save.") and re-follows the thread, so the card
   renders as saved with the switch ON. Now flip the switch OFF, then ON
   again. Toasts "Stopped following" then "Following again"; the row
   regains "· following".
7. **Each remaining knob combination**, on three more fresh threads:
   "Just this message" + ON (body `scope:"message", follow:true`; the
   count in the toast is 1); "Whole thread" + OFF (`follow:false`; the
   saved row reads "In your ocean · N messages saved" with the switch
   rendered OFF; reopening shows the knobs); "Just this message" + OFF
   (`scope:"message", follow:false`).
8. **Single-message thread.** Open one. The radio reads "Whole thread · 1
   message".
9. **Backend down** (point `BACKEND_BASE_URL` at an unreachable host, or
   block the route). Open any thread: the card still renders, with the
   knobs. Tap Save: "Could not save this thread (0)." as before.
10. **Re-save on the saved card sends the switch, not the scope.** With
    the backend's request log open, on a card in the saved state: tap
    Save with the switch ON — body `{item_ids:[…], follow:true}`, no
    `scope`; toast "Already in your ocean — nothing new to save.", row
    still "· following". Flip the switch OFF ("Stopped following"), tap
    Save again — body `{item_ids:[…], follow:false}`, no `scope`; the
    row stays WITHOUT "· following" and the switch stays OFF (the
    re-save did not re-follow). Reopen: the save card (GET said
    `false`).

## Files

| Path | Role |
|---|---|
| `appsscript.json` | Add-on manifest — scopes, the contextual trigger, the homepage trigger, and the two deploy-time allowlists. **Every key audited against Google's current manifest reference on 2026-09-13** — see [`docs/connectors/gmail-addon-publishing.md`](../../docs/connectors/gmail-addon-publishing.md) §1. Installed and run for real via **Test deployments** (PR #835); still never installed from a *versioned* deploy — see "What is and isn't verified" below. |
| `Config.gs` | Backend/web/Clerk host constants, **`CLERK_CLIENT_ID`**, `redirectUri_()` / `logRedirectUri()`, and `PropertiesService` key names. |
| `Auth.gs` | Clerk OAuth: the OAuth2 service (PKCE, no secret), `currentCredential_()` (which bearer to send), `authCallback`, `signOut`, `changeWorkspace`, `universalSignOut`, and the legacy-token retirement. |
| `Http.gs` | `UrlFetchApp` wrapper shared by every backend call. |
| `Card.gs` | The two entry points (`onGmailMessageOpen`, `onHomepage`) and every `Card` builder, including the sign-in card. |
| `Ingest.gs` | `saveToCoralSwarm` / `toggleFollow` — the message card's actions: the save (with the `scope`/`follow` options) and the live follow switch (`PUT /v1/connectors/google/watched/{threadId}`). |
| `Settings.gs` | `toggleEverythingMode` / `confirmEverythingMode` / `cancelEverythingMode` — the homepage's whole-mailbox switch, driving the pre-existing `GET`/`POST /v1/connectors/google/gmail/everything-mode` routes with the signed-in user's credential. |
| `Calendar.gs` | Google Calendar surface: `onCalendarEventOpen` / `onCalendarEventUpdate` / `onCalendarHomepage`, the Meet-code reader (`meetFromCalendarEvent_`), the event card, and the `approveMeetCapture` / `undoMeetCapture` / `refreshCalendarCard` actions. See "Google Calendar: approve a Meet meeting". |
| `scripts/publish-wizard.sh` | **Not deployed** — an interactive checklist that walks a human through the Cloud-console / Marketplace / Admin-console steps of publishing. Read-only. |

There is no `package.json`, no build step, and no bundler: Apps Script
compiles every `.gs` file in the project together into one global scope, so
the manifest plus the `.gs` files are the deployable unit as-is (paste, or `clasp push` —
see below).

## UI: how the cards are styled, and what they cannot be

Since 2026-09-14 the cards follow "row C" of the design canvas
(https://claude.ai/code/artifact/00000000-0000-4000-8000-ff5d680985a7): Google
Keep's Gmail panel as the model, the web app's cool-neutral system
(`web/app/globals.css` `:root`) as the palette. A Gmail add-on is a
declarative `Card` — no DOM, no CSS — so the design lives in exactly these
levers, and nothing else is ours to change:

| Lever | Where | Value |
|---|---|---|
| Top bar colour + title text | `appsscript.json` `layoutProperties.primaryColor` | `#F5F6F8` (web `--bg-2`); Gmail picks a dark title. **Pure `#FFFFFF` is ignored by Gmail.** A manifest layout change only shows after **Deploy → Test deployments → Uninstall → Install** plus a Gmail reload. |
| Button / link colour | `layoutProperties.secondaryColor` + `Card.gs` `BRAND_COLOR` | `#F77764` (web `--brand`), set explicitly on every FILLED button. |
| Row icons | `web/public/addin/icons/<name>--<hue>.png?v=N`, via `IconImage.setIconUrl` | The web app's stroke set (1.8px, round caps) rendered at 96px per hue (`ink`, `coral`, `teal`, `plum`, `pink`, `amber`, `ok`, `muted`) — render them in a browser canvas, NOT with macOS Quick Look, which ignores the SVG viewBox and leaves a 24px glyph in the corner. Hosted URLs only — no data URIs. Google's image proxy caches by full URL: bump `ICON_VERSION` in Card.gs whenever a PNG's content changes. |
| Empty states | `web/public/addin/empty-start.png`, `empty-home.png`, via `Image` | One rendered image each (soft coral circle + mandala + Instrument Serif title + hint, ~2.3×); the `altText` carries the words. CardService can neither centre text nor set a font. |
| Copy | the `.gs` literals | Sentence case, verbs first, the ocean lexicon. |

Fixed by Gmail, not by us: the font (Roboto), text and label colours, pill
buttons, row height and spacing, the ⋮ / × chrome.

**Deploy order for any change that adds or renames an asset:** merge → web
deploy (`gh workflow run Deploy --ref main -f target=both`, after the
invariant-guard scripts on a detached `origin/main` worktree) → **then** push
the `.gs` files to the Apps Script project. An icon or image URL that does
not resolve renders as a broken image, not a fallback.

The homepage's "Ingest my whole mailbox" switch is the only mailbox-level
setting on the cards today; the message card carries the per-save `scope`
radio and `follow` switch ("Save options and following" above). Auto-ingest
rules (sender / label / subject / custom query, each with the same scope +
follow knobs as a manual save) are designed on the same canvas and land with
their backend.

## Sign-in (Clerk OAuth)

The add-on signs in with Clerk using Apps Script's third-party-auth pattern
([apps-script-oauth2](https://github.com/googleworkspace/apps-script-oauth2),
library ID `1B7FSrk5Zi6L1rSxxTDgDEUsPzlukDsi4KGuTMorsTQHhGBzBkMun4iDF`,
pinned at version 43 in the manifest). Live-verified in Gmail web against the
dev Clerk instance.

- **Flow:** Google add-on consent → the sign-in card (a CardService
  authorization action) → Clerk sign-in (Google) → Clerk consent with the
  workspace (organization) picker → redirect to
  `https://script.google.com/macros/d/<SCRIPT_ID>/usercallback` → token
  exchange in `authCallback` (Auth.gs).
- **Public client, PKCE, no secret.** `generateCodeVerifier()` supplies the
  S256 challenge. `setTokenPayloadHandler` deletes `client_secret` from every
  token request; a placeholder secret is still *set* because library v43's
  `refresh()` throws without one. Nothing secret exists in the repo.
- **Scopes:** `openid profile email offline_access user:org:read`.
  `offline_access` gets a refresh token; `user:org:read` is what makes Clerk's
  consent screen show the workspace picker. The pick is the access token's
  `org_id`.
- **Token:** a JWT access token carrying the add-on's `client_id` and the
  picked `org_id` (no `sid`), 24h. The backend (env
  `GMAIL_ADDIN_CLERK_OAUTH_CLIENT_ID`) accepts it as a Bearer on exactly the
  routes the legacy `csat_` token reaches. `Http.gs`'s `callBackend_` is
  unchanged; every caller passes `currentCredential_().token`.
- **Refresh:** `hasAccess()` refreshes transparently. Refresh tokens
  **rotate**, so the per-user `LockService` lock around refresh must stay;
  without it two simultaneous executions replay one refresh token and one gets
  `invalid_grant`. Tokens persist in User Properties (`oauth2.clerk`).
- **Cards:** signed-in cards show the workspace (the token's org name if it has
  one, else the `org_id`, else "Personal") with **Change workspace** (resets the
  tokens and re-authorizes; `prompt=consent` is always sent, so the picker
  shows again) and **Sign out** (`service.reset()`; also in the ⋮ menu as a
  universal action). Neither revokes the grant at Clerk.
- **Manifest:** `urlFetchWhitelist` gained `https://clerk.coralswarm.example/` (the
  library's token POST), `openLinkUrlPrefixes` gained the same prefix (carried
  over from the spike, which verified with it; the authorization action may not
  need it), `dependencies.libraries` gained OAuth2, and a `universalActions`
  entry was added for sign-out. **`oauthScopes` is unchanged**: it already had
  the three the spike needed (`gmail.addons.execute`,
  `gmail.addons.current.message.metadata`, `script.external_request`), so
  Google re-consent is not triggered by this change.

### Setup: Clerk OAuth app and redirect URI

1. In the production Clerk dashboard (instance FAPI `https://clerk.coralswarm.example`)
   create an OAuth application: **public client**, PKCE required, no secret,
   scopes `openid profile email offline_access user:org:read`.
2. Register its redirect URI:
   `https://script.google.com/macros/d/<SCRIPT_ID>/usercallback`, where
   `<SCRIPT_ID>` is the Apps Script project's script ID. Run `logRedirectUri`
   from the editor to print the exact value. **A new Apps Script project is a
   new script ID and therefore a new redirect URI — register it on the Clerk
   app before anyone signs in from that project.**
3. Put the app's client id in `Config.gs` (`CLERK_CLIENT_ID`) — or set the `CLERK_CLIENT_ID` Script property — and set the
   same value as the backend's `GMAIL_ADDIN_CLERK_OAUTH_CLIENT_ID`. If it is
   `REPLACE_ME`, signed-out users see a setup card.
4. For a non-production Clerk instance, set the `CLERK_FAPI_BASE_URL` Script
   property and add that host to `urlFetchWhitelist` and `openLinkUrlPrefixes`
   (same trap as `BACKEND_BASE_URL` below).

**Production values.** Apps Script project `EXAMPLE_APPS_SCRIPT_ID`
("CoralSwarm Gmail…", owned by person@example.com). Clerk app "CoralSwarm for
Gmail", client id `example_15` (set in `Config.gs`; backend env
`GMAIL_ADDIN_CLERK_OAUTH_CLIENT_ID=example_15`). Registered redirect URI:
`https://script.google.com/macros/d/EXAMPLE_APPS_SCRIPT_ID/usercallback`

### Migration from device pairing

The pairing flow (`Pairing.gs`, `pair/start` / `pair/poll` / the web approve
page) is gone from the add-on. Users who are already paired keep a stored
`csat_` token, and **it keeps working** until they sign in with Clerk:

- `currentCredential_()` (Auth.gs) prefers a valid Clerk access token and falls
  back to the legacy `csat_` token. Both reach the same routes.
- A legacy-connected card shows "Legacy connection" and a **Sign in with
  CoralSwarm** button (once `CLERK_CLIENT_ID` is set).
- On a successful Clerk sign-in, `authCallback` revokes the legacy token
  server-side (`DELETE /v1/addin/tokens/{id}`, best effort) and deletes it
  locally. **Sign out** does the same for a legacy-only user.
- If the server-side revoke fails, the legacy token (and its id, which the revoke needs together with the token as bearer) is kept so **Sign out** can retry; the sign-in page and toast say so and point at Connectors → Gmail Add-on. The revoke always uses the `csat_` token itself as the bearer, never the Clerk token (the backend does not allow `DELETE /v1/addin/tokens/{id}` for a Gmail OAuth token).
- Organization workspaces: the backend's watched routes are personal-only, so with an org-workspace Clerk token the card hides the follow switch (save sends `scope` only).
- A 401 on either credential drops that credential and shows the sign-in card.
- The legacy `PROP_ADDIN_*` keys in Config.gs and the backend pairing routes
  can be deleted once no one is still on a `csat_` token; the web approve page
  (`web/app/(app)/addin/approve`) is still used by the Outlook/Teams panes.

## Local dev / testing

Apps Script has two ways to run this without a Marketplace listing:

1. **The Apps Script editor's own "Test deployments" flow** (no tooling
   needed): create a new Apps Script project at
   [script.google.com](https://script.google.com), paste in
   `appsscript.json` (via the project's manifest file — enable "Show
   `appsscript.json`" in Project Settings first) and each `.gs` file, then
   **Deploy → Test deployments → Install**. That installs the add-on for
   your own Google account only, and Gmail picks it up on next reload — no
   review, no publishing.
2. **[`clasp`](https://github.com/google/clasp)** (Google's Apps Script
   CLI), if you want to push from this directory instead of pasting: `npm
   i -g @google/clasp` (Node >= 22), enable the Apps Script API at
   <https://script.google.com/home/usersettings>, `clasp login`,
   `clasp create-script --type standalone --title "CoralSwarm Gmail Add-on"`
   (or `clasp clone-script <scriptId>` against an existing project), then
   `clasp push`. **clasp v3 renamed most v2 commands** (`create` ->
   `create-script`, `clone` -> `clone-script`, `open` -> `open-script`) — see
   [`docs/connectors/gmail-addon-publishing.md`](../../docs/connectors/gmail-addon-publishing.md)
   §4 for the current command set. **Not set up in this repo** — no
   `.clasp.json`, no `clasp` npm dependency committed — since it needs a
   `scriptId` tied to a real Apps Script project and an OAuth login this
   repo can't carry. Documented as an option, not built as tooling, the same
   way the Outlook add-in's own README documents its Fly deploy as an
   operator step outside this repo.

Either way, point it at a non-production backend first via the Apps Script
editor's **Project Settings → Script properties**: add `BACKEND_BASE_URL`
and/or `WEB_BASE_URL` (see `Config.gs`'s `backendBaseUrl_()`/`webBaseUrl_()`)
— no code edit needed to test against staging.

**The manifest's `urlFetchWhitelist` must ALSO list the non-default
`BACKEND_BASE_URL`, or every backend call fails closed.** `urlFetchWhitelist`
is a static, deploy-time allowlist of HTTPS prefixes `UrlFetchApp` may reach
(`appsscript.json`, currently `https://api.coralswarm.example/` and `https://clerk.coralswarm.example/`) — it is
evaluated against the manifest as deployed, not against whatever
`BACKEND_BASE_URL` script property happens to be set at runtime. Pointing
`BACKEND_BASE_URL` at a staging host with no matching whitelist entry does
NOT get a permission prompt or a clear error: `UrlFetchApp.fetch` throws
before the request ever leaves the script, `callBackend_` (Http.gs) catches
that and returns `status: 0`, and the card just shows a generic
failure — indistinguishable from a real network
failure. Add the exact HTTPS prefix (trailing slash, full path — see
[Google's URL allowlist docs](https://developers.google.com/apps-script/manifest/allowlist-url))
for whatever `BACKEND_BASE_URL` you're testing against before "Test
deployments → Install"-ing that config. `webBaseUrl_()`'s host does NOT need an entry
in `urlFetchWhitelist` — it only builds the `CardService.setOpenLink` URL,
never an `UrlFetchApp` target — **but it DOES need one in
`addOns.common.openLinkUrlPrefixes`**, which is the exact same trap wearing a
different name. `openLinkUrlPrefixes` is a second static, deploy-time allowlist,
required whenever an add-on renders an `OpenLink` (which `Card.gs` does, for the
"In your ocean" deep link), and it is evaluated against the manifest as deployed
rather than against the `WEB_BASE_URL` script property. Point `WEB_BASE_URL` at
staging without adding that host's prefix and the link button silently fails
to open.
See [`docs/connectors/gmail-addon-publishing.md`](../../docs/connectors/gmail-addon-publishing.md)
§1.2.

## What is and isn't verified (read before deploying for real)

> The sections below record the verification of the **device-pairing** version
> (PRs #835/#905). The Clerk sign-in flow was verified separately in a spike
> against the dev Clerk instance (see "Sign-in (Clerk OAuth)"); it has not yet
> been re-run against the real add-on in production.

### Verified live, in a real Gmail account (PR #835)

The add-on **has been installed and run end to end against production** —
not just read for syntax. Installed through the Apps Script editor's **Test
deployments → Install** flow (option 1 under "Local dev / testing") into a
real Gmail account, then driven through the whole flow: Connect → approval
page in the browser → Approve → "I've approved — continue" → open a message
→ Save to CoralSwarm. The backend logged the ingest (`ingested:1`), so the
path this README describes is a path that has actually run, including the
`csat_` PAT authenticating `POST /v1/connectors/google/ingest`.

The manifest shape is therefore confirmed to install — **with one correction
that only a live install could have found**: `oauthScopes` was missing
`https://www.googleapis.com/auth/gmail.addons.execute`, and without it every
card action failed at runtime with "The script does not have permission to
perform that action". Adding exactly that scope fixed it (PR #835). Nothing
else in the manifest needed changing.

### Verified against Google's current docs (PR #905)

- ~~**The `appsscript.json` manifest shape itself.**~~ **RESOLVED
  2026-09-13** — every key was audited against Google's current manifest
  references; see
  [`docs/connectors/gmail-addon-publishing.md`](../../docs/connectors/gmail-addon-publishing.md)
  §1 for the key-by-key table with citations. Nothing in this manifest is
  renamed, restructured, or deprecated, and all three `oauthScopes` are real
  and current. One **required field was missing** and has been added:
  `addOns.common.openLinkUrlPrefixes`, mandatory because `Card.gs` renders an
  `OpenLink` — without it the pairing approval button would have been blocked
  in a versioned deployment. Note how this sits beside the live install
  above: that run got all the way through the approval link with this key
  ABSENT, on a Test deployment — so a passing manual test was never evidence
  the manifest was complete, and #905 found the gap by reading Google's
  reference rather than by running anything. The two checks cover different
  things and neither substitutes for the other.
- ~~**Real install path (Marketplace vs. domain-internal) is
  unresearched.**~~ **RESOLVED 2026-09-13** —
  [`docs/connectors/gmail-addon-publishing.md`](../../docs/connectors/gmail-addon-publishing.md)
  is the full writeup, and
  [`scripts/publish-wizard.sh`](./scripts/publish-wizard.sh) walks the
  dashboard-only steps. Short version: coralswarm.example **is** a Google Workspace
  domain, so the first move is a **private Marketplace listing** into that org —
  no Google review, no OAuth verification, live on publish. A public listing
  later needs brand verification plus app review (triggered by
  `gmail.addons.current.message.metadata`, a *sensitive* scope), but **no CASA
  security assessment**, because none of the three scopes is *restricted*.
  Public-vs-private visibility is permanent per Cloud project, so the public
  listing will be a second project. **Neither path is set up yet** — the runbook
  exists, the dashboards are untouched.

### Still open

- **The on-open following read has no timeout.** `threadFollowing_`
  (`GET …/watched/{threadId}`) is a synchronous `UrlFetchApp.fetch` inside
  the contextual trigger, and `UrlFetchApp` exposes no per-request timeout.
  A backend that *refuses* (connection error, non-2xx) degrades cleanly to
  the save card — but a backend that *hangs* holds the trigger until Apps
  Script's add-on execution budget (~30 s) expires, and Gmail then shows its
  generic "something went wrong" instead of any card. The homepage's
  `everythingModeEnabled_` read has had exactly this exposure since it
  shipped; this adds it to the message card too.
- **One account, one installer, one install method — and never a *versioned*
  deploy.** "Test deployments" installs for the developer's own Google
  account only, and runs the head of the script project rather than a
  published version. Nothing here has been through a Marketplace listing (of
  either visibility), a domain-wide admin install, OAuth verification, or
  anyone else's mailbox. The publishing runbook above is a plan that has not
  been carried out.
- **The logo URL: asset shipped, deploy pending.** `addOns.common.logoUrl` is
  `https://app.coralswarm.example/addin/gmail-icon.png`, and that path is served
  by the web app as of **PR #906** (`web/public/addin/gmail-icon.png`). It is
  a **required** manifest field and must be a publicly fetchable HTTPS image
  (128×128, square, transparent — it doubles as the Marketplace listing
  icon), so the thing to confirm before the first *versioned* deployment is
  that the web deploy carrying #906 has actually landed: the URL was still
  `404` when checked on 2026-09-13, with the PNG already merged. The ordering,
  not the asset, is the open item. The previous value —
  `https://addin.coralswarm.example/assets/gmail-icon.png` — was never viable:
  `addin.coralswarm.example` is not a real app, it 308-redirects every path to
  `app.coralswarm.example` (verified with `curl`), so nothing was ever there to
  fetch. Apps Script silently blanks an icon it cannot fetch, so a `curl`
  check is the only way to know.

### The approve race — mitigated, not closed

`POST /v1/addin/pair/approve` is keyed on the pairing `code` alone. That was
a knowing trade-off when this shipped (#820): an attacker who guesses a
currently-live 8-character code before its owner approves it can approve it
themselves, binding the pairing to the ATTACKER's account. They never learn
the victim's token — the plaintext only ever goes to whoever holds
`poll_secret` — but the inverse is real: the victim's saved threads would
land in the attacker's ocean.

Two places now name the account instead of leaving that silent, and they are
different halves of the same fix:

- **Before approving**, the web approval page says "Approving as
  &lt;email&gt;" from the signed-in Clerk session, and repeats it in the
  confirmation (PR #906, `web/lib/addinApprove.ts`) — the browser you approve
  in is routinely signed into a different account than the Gmail you are
  approving *for*.
- **After approving**, `pair/poll`'s `approved` response carries
  **`account_email`**, the `users.email` of whoever actually approved, and
  the connected card in Gmail renders "**Connected as &lt;email&gt;**". A
  hijacked pairing therefore names an address the user does not recognize,
  on the first card they see after connecting.

That is a detection control: it makes the failure visible and undoable
(Disconnect, or Revoke on Connectors), it does not prevent the race.
Preventing it means binding the pairing attempt to the approving session
before the code is ever displayed — a larger change to the handshake, still
not done. The per-`code` rate limit on both `pair/approve` and `pair/poll`
slows the guessing down and nothing more (see
`backend/src/routes/addin.rs`, `PAIR_CODE_RATE_MAX_PER_WINDOW`).

### Revoking: from Gmail, and now from the web

Both paths hit the same route:

- **From Gmail**: `disconnect()` calls `DELETE /v1/addin/tokens/{id}` with
  the stored `token_id`, using the token itself as its own bearer, and its
  notification reflects whether that call actually succeeded (CodeRabbit
  review on #820, Major 5).
- **From the web app**: Connectors → **Gmail Add-on** lists this account's
  live tokens with a per-row Revoke button
  (PR #906, `web/components/connectors/GmailAddonTokens.tsx`), reading
  `GET /v1/addin/tokens` (owner- and account-scoped, `revoked_at IS NULL` in
  the query, never a hash) and calling the same
  `DELETE /v1/addin/tokens/{id}`. This is the case Gmail-side disconnect
  cannot serve — "the device with the add-on installed is the one I no
  longer control". `GET /v1/addin/tokens` is the newest piece of the three:
  until a backend carrying it is deployed, that section renders an honest
  "not available on this backend yet" line rather than an empty list
  (`addinListErrorCopy`) — an absent list must never read as "nothing is
  paired".

## Google Calendar: approve a Meet meeting

Product rule: a Google Meet transcript is saved only for a meeting the user
explicitly approves, and approval lives only where the meeting happens: here
(creating or editing the event in Calendar) or in a Meet side panel (a separate
add-on). There is no approval screen in the web app.

Manifest (`addOns.calendar`): `currentEventAccess: "READ"`, `eventOpenTrigger`
-> `onCalendarEventOpen`, `eventUpdateTrigger` -> `onCalendarEventUpdate`,
`homepageTrigger` -> `onCalendarHomepage`. Added scopes:
`calendar.addons.execute` and `calendar.addons.current.event.read`. **Existing
users must re-consent** the next time they open the add-on (Google prompts for
the new scopes), and the Marketplace listing / OAuth consent screen must list
them (`calendar.addons.current.event.read` is a sensitive scope).

The card, per event:

| Event | Card |
|---|---|
| Meet link, not approved | "Save this meeting to your CoralSwarm ocean?", the code, **Approve** (`POST captures {meeting_code}`) |
| Approved (found by `GET captures`, matched on `meeting_code`) | "Approved. The transcript saves after the meeting (transcripts must be on)", **Undo** (`DELETE captures/{id}`) |
| Already saved (`ingested`), one-off event | "Saved to your ocean" (no Undo; the copy stays) |
| Recurring instance (`recurringEventId`) | adds "Approves the next occurrence only." An earlier saved occurrence does not hide Approve |
| No Meet | "Add Google Meet video conferencing to this event first." + **Check again** |
| 409 `google_meet_not_connected` | "Connect Google Meet" OpenLink to `/connectors` |
| 403 (`org_google_meet_disabled`) | "not enabled for this workspace" |
| Signed out / 401 | the existing Clerk sign-in card |

Event title: the card shows the event's title ("Save "<title>" to your CoralSwarm
ocean?") with "Meet · abc-defg-hij" as a secondary line. Calendar's add-on event
object has no title field and `calendar.addons.current.event.read` does not
unlock one, and `Calendar.Events.get` does not accept the add-on scopes. So the
title is read with the Calendar Advanced Service (`appsscript.json`
`dependencies.enabledAdvancedServices`: `Calendar`, `calendar`, `v3`) under the
extra scope `calendar.events.readonly` (sensitive; existing users re-consent;
the OAuth consent screen must list it). `calendarEventTitle_` requests
`fields: summary` only and never throws: an unsaved event (no `calendar.id`), an
untitled event, a missing grant or any API error falls back to "This meeting" and
Approve still works. The title is carried to the post-action cards in the action
parameters (clipped to 120 chars), so Approve/Undo do not re-read it. A narrower
alternative is `calendar.events.owned.readonly` (events on calendars the user
owns, which covers invitations on their primary calendar).

Scope consent for already-installed users: Google does not re-prompt for a
scope added to the manifest later, so `Events.get` fails with "You do not have
permission to call calendar.events.get". Every Calendar trigger (open, update,
homepage) therefore calls `ScriptApp.requireScopes(FULL, [calendar.events.readonly])`
first (`requireCalendarTitleScope_`), which ends the execution and renders
Google's authorization prompt when the grant is missing. It is not called from
Approve/Undo. A user who declines sees the prompt instead of the card until they
grant it.

Consent screen name: the name Google shows on the consent screen ("CoralSwarm
Gmail Add-on") is NOT `addOns.common.name` (already "CoralSwarm"); it comes from
the GCP project's OAuth consent screen (Google Auth platform -> Branding -> App
name) and, for a script on the default project, the Apps Script project title
(Project Settings). Rename both in the console; nothing in the repo controls it.

What Calendar exposes (Google docs, "Event objects" -> Calendar event object):
`e.calendar.conferenceData` is present only if the data is in the event AND
`currentEventAccess` is READ/READ_WRITE. The meeting code is read from a `video`
entry point's URI (`https://meet.google.com/abc-defg-hij`), else an entry
point's `meetingCode`, else `conferenceId` when the solution type is
`hangoutsMeet`. `calendar.id` is absent on an unsaved event;
`calendar.recurringEventId` is set only on instances of a saved series.

New / unsaved events: Calendar generates the Meet link when the event is saved,
so a freshly added Meet may arrive as a `hangoutsMeet` solution with no code
(the card says to save the event, then **Check again**). `eventUpdateTrigger`
(fires before save when conferencing or attendees change) re-renders the card
for the case where the code is already present. For an unsaved event the
recurrence is not exposed, so the card says "If this event repeats, approval
covers the next occurrence only." This unsaved-event behaviour is built from the
docs and the unit harness only; it is **not verified in a live Calendar** (see
the PR checklist).

Backend: `POST/GET /v1/connectors/google/meet/captures` and
`DELETE .../captures/{id}` (`routes::google_meet`), allowed for this add-on's
Clerk token by `gmail_scope_allows`. The member must have connected Google Meet
on the web Connectors page ("Connect Google Meet"), because the backend reads
the transcript with the member's own Meet token.

## Backend surface this add-on depends on

- `POST /v1/connectors/google/ingest` (`routes::connectors::ingest_selected`)
  — body `{item_ids:[messageId]}` plus `scope: "thread" | "message"`
  (default `thread`) and `follow: true | false` (default `true`), each
  sent iff its control was rendered on the card that fired the save (see
  the table under "Save options and following"). Response unchanged
  (`{ingested, fetched}`). Omitting both is the pre-options behaviour; on
  an already-saved thread the backend dedupes the messages and sets
  `follow_replies` to the request's value.
- `GET /v1/connectors/google/watched/{threadId}` → `200 {"following": bool}`
  — `false` for a thread never saved OR unfollowed (never a 404 for "not
  saved"). Read once per message-card open by `threadFollowing_`.
- `PUT /v1/connectors/google/watched/{threadId}` body `{"follow": bool}` →
  `200 {"following": bool}`; `404` when the thread has no watched row.
  Called by `toggleFollow` from the saved card's live switch.
- `POST /v1/addin/pair/start`, `POST /v1/addin/pair/poll` — new, UNAUTHENTICATED.
- `POST /v1/addin/pair/approve` — new, AUTHENTICATED (called from the web
  approval page, never from this add-on directly).
- `GET /v1/addin/tokens` — lists the CALLER'S OWN live tokens
  (`{"tokens":[{"id","name","created_at","last_used_at"}]}`), owner- and
  account-scoped from the authenticated session, never `token_hash`,
  personal-workspace only (400 in an org workspace, same as
  `pair/approve`). Accepts a Clerk JWT or a `csat_` PAT. Consumed by the
  `/connectors` page (see "Revoking: from Gmail, and now from the web").
- `DELETE /v1/addin/tokens/{id}` — AUTHENTICATED (accepts a `csat_` PAT
  bearer too, per `require_auth`'s second bearer shape), called for real by
  the Gmail Add-on's own `disconnect()` AND by the `/connectors` page's
  per-row Revoke button.

All five `addin` routes, their two migrations (`0210_addin_tokens`,
`0211_addin_pairing_codes`), and the `require_auth` bearer-shape change live
in `backend/src/routes/addin.rs` and `backend/src/middleware/auth.rs` — see
that module's doc comment for the security properties (pairing-code vs.
poll-secret split, single-retrieval guarantee, personal-plane-only scope)
and `backend/src/routes/addin.rs`'s own `addin_pg_tests` / the standalone
`backend/tests/addin_auth_pg.rs` for the pairing-flow and middleware-boundary
proofs against a real Postgres.
