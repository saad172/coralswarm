# CoralSwarm for Google Meet (side panel add-on)

"Add to CoralSwarm", inside a Google Meet call. The member opens CoralSwarm
from Meet's Activities panel, signs in once, and approves THIS meeting for
their own transcript copy.

Founder decision (2026-10-06): a Meet transcript is saved only for a meeting
the member explicitly approves. Approval lives in Google Calendar (the Gmail
add-on's Calendar card, `extensions/gmail-addin/Calendar.gs`) and here, inside
the meeting. There is no approval screen in the web app.

## What is where

| Piece | Location |
| --- | --- |
| Deployment descriptor | `deployment.json` (this folder) |
| Deploy helper | `deploy.sh` (this folder) |
| The panel page | `web/app/meet/sidepanel/` (served at `https://app.coralswarm.example/meet/sidepanel`) |
| Panel UI | `web/components/addin/MeetSidePanel.tsx` |
| Meet SDK wrapper | `web/lib/addin/meetSdk.ts` (`meet.addons.js` 1.2.0, cloud project number `195592663815`) |
| Sign-in | `web/lib/addin/meetClient.ts`, `authDialog.ts` (`meet` popup target), `clerkOAuth.ts` (state marker `g`) |
| Backend scope | `backend/src/routes/addin_microsoft.rs` `meet_scope_allows`; classification in `backend/src/middleware/auth.rs` |
| Capture routes | `backend/src/routes/google_meet.rs` (`POST/GET /v1/connectors/google/meet/captures`, `DELETE .../{id}`) |

The panel is our page in an iframe on `meet.google.com`. `next.config.ts`
sends `Content-Security-Policy: frame-ancestors https://meet.google.com` for
`/meet/*`.

## How it works

1. The panel loads the Meet add-ons SDK and reads this meeting:
   `createAddonSession({cloudProjectNumber})` -> `createSidePanelClient()` ->
   `getMeetingInfo()` = `{meetingId: "spaces/…", meetingCode: "abc-defg-hij"}`.
2. **Sign in** (Clerk OAuth, authorization code + PKCE, public client, the
   Meet app's own client id). Meet has no dialog API, so the click handler
   synchronously opens `about:blank` in a popup, then navigates it to
   `/addin/auth/start?host=meet&state=g…`. Clerk redirects to
   `/addin/auth/callback`, which `window.opener.postMessage({code, state},
   origin)`s and closes itself. The panel accepts a message only from our
   origin, from that popup, with its state; a closed popup is a cancel. The
   panel redeems the code directly (Clerk's token endpoint allows CORS).
   The tokens persist in the panel origin's localStorage, which Chrome
   partitions by (app.coralswarm.example, meet.google.com), so the member signs in
   once per browser and the next meeting's panel resumes without the popup.
   Sign out clears the record; when storage is blocked the panel is memory-only.
3. `GET /v1/addin/workspace` names the workspace in the footer;
   `GET .../captures` shows whether this meeting (by space or code) is already
   approved.
4. **Add to CoralSwarm** = `POST .../captures {"space": meetingId}`.
   **Undo** = `DELETE .../captures/{id}`. The Google Meet sweep later writes the
   member's OWN copy a few minutes after the meeting ends, if a transcript
   exists. 409 `google_meet_not_connected` shows "Connect Google Meet"
   (opens `/connectors`); 403 `org_google_meet_disabled` says the org has not
   enabled Meet.

Transcript on/off is not read from the API (the conference record is only
visible to the organizer); the panel shows a static hint instead
("Activities → Transcripts").

## Configuration

| Where | Name | Value (prod) |
| --- | --- | --- |
| Clerk app | "CoralSwarm for Google Meet" | client id `example_18`, redirect `https://app.coralswarm.example/addin/auth/callback` |
| Clerk app (dev) | | client id `O9PJBOvgsF9ZWQiO`, localhost:3000 redirects |
| Backend (Fly secret) | `MEET_ADDIN_CLERK_OAUTH_CLIENT_ID` | `example_18` |
| Web build env | `NEXT_PUBLIC_MEET_ADDIN_CLERK_OAUTH_CLIENT_ID` | `example_18` (`deploy.yml`, `scripts/deploy_local.sh`) |

Without the backend secret, a Meet token is classified as a foreign OAuth
token (`REST_FOREIGN_OAUTH` decides: warn lets it through unscoped, reject
401s it). Without the web env, the panel says sign-in is unavailable.

## Deploying the add-on

The web app must be deployed first (the panel URL has to serve). Then, from
this folder, logged in to gcloud with access to project `coralswarm-workspace`:

```bash
./deploy.sh create     # first time only (deployment id: coralswarm-meet)
./deploy.sh replace    # after editing deployment.json
./deploy.sh install    # install for your own account to test
./deploy.sh status
```

Override the project or id with `MEET_ADDON_PROJECT` / `MEET_ADDON_DEPLOYMENT_ID`.

Test: start or join a Meet call in the account that installed it, open
Activities (the shapes icon) -> CoralSwarm, sign in, Add, Undo.

The spike deployment `cs-meet-spike` (2026-10-06) still exists in the same
project; uninstall and delete it once this one is installed:
`gcloud workspace-add-ons deployments uninstall cs-meet-spike --project=coralswarm-workspace`
then `... deployments delete cs-meet-spike ...`.

Publishing to other users (Google Workspace Marketplace listing, OAuth
consent screen verification) is a separate step and not covered here.
