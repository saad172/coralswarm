# CoralSwarm Outlook add-in

Two ribbon buttons in Outlook, one system with the Teams meeting pane and the
Gmail add-on:

| Surface | Button | Pane (Next.js route) |
|---|---|---|
| Reading a message | **Save to CoralSwarm** | `/outlook/mail` (`web/components/addin/OutlookMailPane.tsx`) |
| Teams appointment (organizer or attendee) | **Save meeting to CoralSwarm** | `/outlook/meeting` (`web/components/connectors/OutlookMeetingPane.tsx`) |

Email is never bulk-read. A message enters CoralSwarm only when the user opens it
and saves it: the pane sends that one message's id, the backend expands it to the
conversation (unless "Just this message" is chosen) and, unless "Keep saving new
replies" is off, registers the thread so later replies follow. The one exception
is **Ingest my whole mailbox**, an explicit opt-in switch behind a confirm card
(`backend/src/routes/outlook_everything_mode.rs`; private ocean only).

## Where the code lives, and why

This package is only the **manifest and the icons**. The panes used to be static
HTML/JS under `src/` (a Clerk-in-the-pane task pane plus an iframe wrapper for
meetings). They are now routes of the web app, because:

- Teams and Outlook are one system and share one UI kit (`web/components/addin/`),
  start card, pairing, account menu and copy; a second plain-JS copy of that kit
  would drift.
- Office SSO / nested app auth cannot run from a nested iframe, so the meeting
  pane can no longer sit in an iframe under a wrapper page; it runs directly in
  the task pane.

Production origin: `https://app.coralswarm.example/outlook`. The static files at that
path (this manifest, `assets/`) are produced by
`web/scripts/build-outlook-addin.mjs` during the web build. The old
separate static host (its own Fly app, Dockerfile and nginx config) is gone.

## Sign-in

1. The pane gets a Microsoft token: nested app auth (MSAL
   `createNestablePublicClientApplication`, scope
   `api://app.coralswarm.example/00000000-0000-0000-0000-000000000000/access_as_user`),
   falling back to `Office.auth.getAccessToken`.
2. It POSTs that token to `POST /v1/addin/microsoft/session` and keeps the
   returned CoralSwarm credential **in memory only**. On expiry or a 401 it
   trades again, silently.
3. First time only: **Connect CoralSwarm** shows a pairing code; the user approves
   once in the browser (`/addin/approve?kind=microsoft`), where they pick the
   workspace. The Teams pane uses the same link.
4. If the backend says `microsoft_sso_disabled`, or the host cannot issue a
   token, the panes run the previous Clerk sign-in unchanged.

SSO/NAA are feature-detected at runtime and deliberately **not** required sets in
the manifest, so older clients keep working through the Clerk fallback.

## Azure app registration (`00000000-0000-0000-0000-000000000000`)

Done in Azure, not by this repo:

- Application ID URI `api://app.coralswarm.example/00000000-0000-0000-0000-000000000000`
- Exposed scope `access_as_user`
- **SPA redirect URI `brk-multihub://app.coralswarm.example`** (nested app auth)
- Authorized client applications (for the `Office.auth` fallback), each with
  `access_as_user`: Microsoft Office `00000000-0000-4000-8000-402213b72cab`,
  Office on the web `00000000-0000-4000-8000-a28eedc2bbc0`, Outlook on the web
  `00000000-0000-4000-8000-9182e2c88ff3`, Outlook mobile
  `00000000-0000-4000-8000-cedbc4c10afb`
- Delegated permissions `openid`, `profile`, `email`, `offline_access`;
  `requestedAccessTokenVersion: 2`

## Build, validate, sideload

```
npm run build:manifest:prod     # manifest.local.xml for https://app.coralswarm.example/outlook
npm test                        # manifest guards (SSO block, URLs, labels, build)
npm run validate                # office-addin-manifest schema validation
```

Local development: run the web app over HTTPS (`cd web && npx next dev
--experimental-https`), `npm run build:manifest:local`, then `npm run sideload`.
`NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY` (optional) sets the Clerk hosts the fallback
sign-in needs in `AppDomains`; `CLERK_ACCOUNTS_HOST` overrides the accounts host.

## Icons

`assets/` holds the PNG icons Office requires (16/32/64/80/128) and
`brand-mark.svg`, generated from the DotMandala geometry:

```
node scripts/build-mark.js --icons   # uses macOS `sips`
```

`brand-mark.svg` is also `include_str!`ed by the backend's invite email
(`backend/src/routes/org_invites.rs`), so do not move it.

## Backend: Microsoft SSO sign-in

The backend lets the Outlook panes sign in without a third-party Clerk cookie. They get the
user's Microsoft token from the host (Office nested app auth) and exchange it at
`POST /v1/addin/microsoft/session` for a short-lived `csms_` bearer (kept in
memory; re-exchanged on expiry or 401); the first time, a one-off "Connect
CoralSwarm" pairing links the identity to a workspace. Teams and Outlook share
one link. Behind `ADDIN_MICROSOFT_SSO_ENABLED` — when off (`404
microsoft_sso_disabled`) fall back to Clerk sign-in. The token must be for the app
`00000000-0000-0000-0000-000000000000` / `api://app.coralswarm.example/00000000-0000-0000-0000-000000000000` (scope
`access_as_user`, Teams clients pre-authorized). Contract, env vars and the
Entra configuration: [`docs/connectors/microsoft-addin-sso.md`](../../docs/connectors/microsoft-addin-sso.md).
