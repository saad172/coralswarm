# CoralSwarm Teams package

The package provides conversation onboarding and meeting-only transcript opt-in.
Conversation capture starts from now after a member mentions CoralSwarm and completes
sign-in. For transcripts, use the meeting tab and explicitly confirm capture in the
authenticated CoralSwarm workspace, or add the bot to that meeting and complete its
meeting capture prompt. Transcribing or recording alone does not grant capture.

The new configurable tab is limited to meeting details and the meeting side panel.
It uses public Teams SDK context only; the backend verifies signed installation
metadata and the member's own Microsoft grant before accepting a meeting selector.
Configuration does not call the capture API. Adding the bundled app can also add
its bot to the meeting; that signed installation is capture intent for the verified
connected installer only. Other members must explicitly enable their own capture.
No personal tabs or new Microsoft permissions are added. The meeting tab is scoped
to private scheduled meetings in group chats. Channel-meeting metadata may require
additional permissions that this package does not request. Recurring meetings are
refused until one occurrence can be verified; a series link is not treated as
unlimited permission.

## Build a package

Use the existing manifest ID when updating an app. In Teams admin center,
this is **External app ID**, not the generated **App ID** used in catalog links.
Using the generated catalog ID creates a duplicate instead of updating the app.
The AAD app ID identifies the OAuth/RSC application and must match the backend
Microsoft client ID. The bot app ID identifies the Bot Framework registration
and must match the backend bot application ID. These can be different
applications; do not substitute a development bot. App IDs are public
identifiers, not secrets.

Inspect the installed catalog version first and choose a higher version for an
update. The template is left unchanged; the selected version goes into the ZIP.

```sh
python3 extensions/teams-app/package.py \
  --teams-app-id YOUR_EXISTING_EXTERNAL_APP_GUID \
  --aad-app-id YOUR_PRODUCTION_AAD_APP_GUID \
  --bot-app-id YOUR_PRODUCTION_BOT_APP_GUID \
  --version HIGHER_MAJOR.MINOR.PATCH \
  --output /tmp/coralswarm-teams.zip
```

The ZIP contains `manifest.json`, `color.png` (192×192), and `outline.png`
(32×32) at its root. The builder validates IDs, version syntax, icon dimensions,
and placeholder substitution, and rejects personal static tabs. It preserves bot
scopes and resource-specific permissions from the checked-in manifest.
If `--bot-app-id` is omitted, it defaults to `--aad-app-id` for deployments that
use one application for both identities. Bot credentials never belong in the
manifest or ZIP.

Run package validation:

```sh
python3 -m venv /tmp/teams-package-tests
/tmp/teams-package-tests/bin/pip install -r extensions/teams-app/requirements-test.txt
/tmp/teams-package-tests/bin/python -m unittest discover -s extensions/teams-app -p 'test_*.py'
```

The tests validate the rendered ZIP against the unmodified public
[Microsoft Teams v1.25 schema](https://developer.microsoft.com/en-us/json-schemas/teams/v1.25/MicrosoftTeams.schema.json)
saved as `MicrosoftTeams.v1.25.schema.json`, and reject the channel feature
declaration when misplaced inside a bot. Packaging itself still uses only the
Python standard library. Channel capability guidance:
[Microsoft's private and shared channel app requirements](https://learn.microsoft.com/en-us/microsoftteams/platform/build-apps-for-shared-private-channels).

## External setup and verification

1. Configure the production Azure Bot registration's Teams channel and messaging
   endpoint to the deployed CoralSwarm handler `/v1/webhooks/teams`. Deploy the
   meeting configuration/content routes on `https://app.coralswarm.example` before publishing.
2. Update the existing app in the authorized tenant's Teams organization catalog
   with the ZIP. Review the requested resource-specific permissions before
   publishing; deploying the web app does not update the Teams catalog.
3. Install the production app in the authorized test team. Confirm the actual
   bot mention appears and a new activation card responds.
4. Follow any sign-in card using the mentioning member's Microsoft identity,
   then correlate a fresh message in that one conversation with private capture.
   Verify repeated mentions preserve existing history settings. An installed
   badge or historical card does not establish current end-to-end capture.

`manifest.json` retains the current Resource-Specific Consent permissions for
channel/chat messages, team members, channel settings, and meeting transcripts.
Package publishing and production Teams installation are separate external
actions from building this artifact.

### Native meeting acceptance checks

In a dedicated scheduled private meeting, verify the actual signed installation
event when adding the bundled app. If Teams also installs the bot, the verified
connected installer may receive meeting-only capture. Saving configuration sends
no direct consent request. Another member opening the shared tab must still sign
in and explicitly select capture; SDK context is not identity or consent. Check
that recording/transcription without either path produces no transcript reads.

### Embedded capture controls

The existing meeting content URL now displays the signed-in member, workspace,
and capture controls directly in the Teams tab or side panel. It does not open
the full CoralSwarm app for Enable, status, or Stop. SDK meeting/chat IDs are
selectors: the authenticated backend resolves and verifies the current meeting
before controls become available. Status checks do not grant permission.

After Enable, the pane reads the member-owned capture status approximately every
15 seconds while visible. It shows the last provider check and retrieval count,
without treating retrieval as proof that downstream storage has completed. Stop
acts on that verified meeting and preserves previously saved transcripts. A
workspace, session, or meeting change invalidates pending requests and removes
old controls. Microsoft account connection may use an authentication window;
regular capture stays in the meeting pane.

### Sign-in: Teams SSO, with the Clerk pane as fallback

The meeting pane no longer asks for a CoralSwarm sign-in in the common case.
`webApplicationInfo` carries the AAD application id and the SSO resource
`api://app.coralswarm.example/<AAD app id>`; the pane calls
`authentication.getAuthToken()`, trades the Microsoft token at
`POST /v1/addin/microsoft/session`, and keeps the returned CoralSwarm credential
in memory only. The first time, the user approves once in the browser
(`/addin/approve?kind=microsoft`) and picks a workspace; Teams and Outlook then
share that one link (`⋮` menu: Change workspace, Disconnect). If the backend
answers 404 `microsoft_sso_disabled`, or Teams cannot issue an SSO token, the
pane runs the previous Clerk flow unchanged.

Azure app registration requirements for SSO (the same application as
`--aad-app-id`):

- Application ID URI: `api://app.coralswarm.example/<AAD app id>`
- Exposed scope `access_as_user` (admins and users)
- Pre-authorized client applications: Teams desktop/mobile
  `00000000-0000-4000-8000-e34a138d45f9`, Teams web
  `00000000-0000-4000-8000-8d36f4e05941`
- Delegated permissions granted: `openid`, `profile`, `email`, `offline_access`
- Access tokens v2 (`requestedAccessTokenVersion: 2`)

Backend contract (`csms_` credential, `ADDIN_MICROSOFT_SSO_ENABLED`, env vars, full Entra
configuration): [`docs/connectors/microsoft-addin-sso.md`](../../docs/connectors/microsoft-addin-sso.md).
