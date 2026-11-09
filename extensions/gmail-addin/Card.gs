/**
 * Entry points (`appsscript.json`'s `homepageTrigger`/`contextualTriggers`)
 * and the Card-building functions they and the actions in Auth.gs /
 * Ingest.gs render. See this add-on's README, "How it works", for the two
 * states these compose into: signed out (Clerk OAuth sign-in) → signed in.
 *
 * Presentation ("row C" of the 2026-09-14 design canvas, modelled on Google
 * Keep's Gmail panel and the web app's cool-neutral system):
 *
 * - No card header: Gmail's own top bar (manifest `primaryColor`, the web
 *   app's `--bg-2`) already names the add-on.
 * - The first tappable thing is a coral "+" row (`plusRow_`) — Keep's "Take
 *   a note" — wired to the same action as the footer button.
 * - Empty states are ONE hosted image each (`web/public/addin/empty-*.png`,
 *   illustration + Instrument Serif title + hint) because CardService can
 *   neither centre text nor set a font; the alt text carries the words.
 * - Rows are `DecoratedText` with the web app's stroke icons, hosted as
 *   PNGs per hue (`web/public/addin/icons/<name>--<hue>.png`).
 * - The one primary action stays in a fixed footer as a FILLED button, per
 *   Google's own add-on guide; Disconnect lives in the card's ⋮ menu.
 */

/** CoralSwarm coral — `--brand` in web/app/globals.css, the one colour the
 *  cards use for FILLED buttons. Set explicitly rather than inherited from
 *  the manifest's `primaryColor`, which is the cool-neutral top bar
 *  (`--bg-2`), not the accent. */
var BRAND_COLOR = '#F77764';

/** Cache-buster for the hosted row icons. Gmail fetches card images through
 *  Google's image proxy, which caches by full URL, so a re-rendered PNG at
 *  the same path keeps serving the old pixels for hours. Bump this whenever
 *  a file under `web/public/addin/icons/` changes content. */
var ICON_VERSION = '2';

/**
 * `Action.setParameters` key on the Save action (the "+" row and the footer
 * button) naming WHICH save options the card rendered, so `saveToCoralSwarm`
 * (Ingest.gs) sends a field iff its control was on the card — the rule the
 * Outlook add-in implements, and the two add-ins must behave identically:
 *
 * * `'shown'` — the save card: `scope` radio + `follow` switch
 *   (`saveOptionsWidgets_`) → both fields are sent.
 * * `'follow_only'` — the saved card: the LIVE `follow` switch only
 *   (`followSwitchRow_`), no radio → `follow` is sent, `scope` is not.
 * * `'scope_only'` — the save card in an organization workspace, where the
 *   follow switch is not offered (the watched routes are personal-only):
 *   `scope` is sent, `follow` is not.
 * * absent — no control rendered → neither field is sent (no card does
 *   this today; kept as the safe fallback).
 *
 * Why a marker and not the form input: an UNCHECKED switch is simply ABSENT
 * from `e.formInput`, so the handler cannot tell "the user turned Keep
 * saving new replies off" from "this card never showed the switch" by
 * looking at `e.formInput` alone. The marker makes that decision explicit
 * — with it, absent `follow` means off.
 */
var PARAM_SAVE_OPTIONS = 'save_options';

/** Minimal HTML escape for values interpolated into card copy —
 *  CardService renders a small HTML subset, so a value that reaches a card
 *  from anywhere but this file's own literals gets escaped on the way in. */
function escapeHtml_(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

/**
 * Subject / sender / thread size of the message the card is open on, read
 * through the `gmail.addons.current.message.metadata` scope the manifest
 * already grants (headers only — never the body). Purely presentational:
 * the thread row on the connected card and the "saved" row after a save.
 * Any failure (no open message, homepage trigger, token missing, scope
 * refused) returns `null` and the row falls back to generic copy — the save
 * itself never depends on this.
 *
 * Returns `{ subject, from, count, threadId }` (`count` and `threadId` may
 * be `null`; `from` is the display name when the header has one, else the
 * address).
 */
function currentMessageSummary_(e) {
  try {
    var messageId = e && e.gmail && e.gmail.messageId;
    var accessToken = e && e.gmail && e.gmail.accessToken;
    if (!messageId || !accessToken) return null;
    GmailApp.setCurrentMessageAccessToken(accessToken);
    var message = GmailApp.getMessageById(messageId);
    if (!message) return null;
    var subject = message.getSubject() || '(no subject)';
    var from = message.getFrom() || '';
    var match = /^\s*"?([^"<]*?)"?\s*<[^>]*>\s*$/.exec(from);
    if (match && match[1]) from = match[1];
    var count = null;
    var threadId = null;
    try {
      var thread = message.getThread();
      if (thread) {
        count = thread.getMessageCount();
        threadId = thread.getId() || null;
      }
    } catch (ignored) {
      count = null;
      threadId = null;
    }
    return { subject: subject, from: from, count: count, threadId: threadId };
  } catch (ignored) {
    return null;
  }
}

/**
 * The Gmail thread id the card is open on — the key of the backend's
 * per-thread opt-in row (`gmail_watched_threads.thread_id`, the Gmail API
 * thread id). `e.gmail.threadId` is what the contextual trigger and every
 * action on the message card hand over under the metadata scope; the
 * `GmailThread.getId()` that `currentMessageSummary_` already resolves is
 * the same id and serves as the fallback. `null` when neither is known
 * (homepage, or the summary failed) — callers then skip everything
 * thread-keyed rather than guess.
 */
function currentThreadId_(e, summary) {
  var id = e && e.gmail && e.gmail.threadId;
  if (id) return String(id);
  return summary && summary.threadId ? String(summary.threadId) : null;
}

/**
 * Whether `threadId` is currently followed for reply auto-capture —
 * `GET /v1/connectors/google/watched/{threadId}` → `{following}`. `false`
 * covers both "never saved" and "saved, then unfollowed" (the route never
 * 404s for a thread it doesn't know), and by design those two render
 * identically: the ordinary save card.
 *
 * One call per card open, no retries. `null` when it cannot be determined
 * (no token, no thread id, network failure, non-2xx, malformed body) — the
 * caller then falls back to the ordinary save card, so this read can never
 * break the card.
 */
function threadFollowing_(threadId) {
  var token = bearerToken_();
  if (!token || !threadId) return null;
  var resp = callBackend_(
    'get',
    backendBaseUrl_() + '/v1/connectors/google/watched/' + encodeURIComponent(threadId),
    null,
    token,
  );
  if (!resp.ok || !resp.body || typeof resp.body.following !== 'boolean') return null;
  return resp.body.following;
}

/**
 * Whether whole-mailbox ingest ("everything mode", `routes::
 * gmail_everything_mode`) is on for this mailbox owner. Read fresh on every
 * homepage render so the switch always reflects the backend, never a
 * cached guess. `null` when it cannot be determined (no token, network
 * failure, non-2xx) — the caller then hides the switch rather than showing
 * a state it cannot vouch for.
 */
function everythingModeEnabled_() {
  var token = bearerToken_();
  if (!token) return null;
  var resp = callBackend_(
    'get',
    backendBaseUrl_() + '/v1/connectors/google/gmail/everything-mode',
    null,
    token,
  );
  if (!resp.ok || !resp.body || typeof resp.body.enabled !== 'boolean') return null;
  return resp.body.enabled;
}

/**
 * The contextual trigger: fires every time the user opens a Gmail message
 * (`unconditional: {}` in the manifest — there is no Gmail-side equivalent
 * of filtering by item type the way an Outlook add-in's `ItemIs` rule does;
 * a Gmail Add-on only ever runs inside Gmail, so "any open message" is the
 * whole condition).
 */
function onGmailMessageOpen(e) {
  return entryCard_(e);
}

/**
 * Homepage card — Gmail renders this when the add-on's icon in the side
 * rail is opened with NO message selected. Same branch as the contextual
 * trigger (the SAME `entryCard_`); `buildConnectedCard_` notices there is no
 * open message and renders the homepage (empty state + mailbox settings)
 * instead of the thread rows and the Save footer.
 */
function onHomepage(e) {
  return entryCard_(e);
}

/**
 * Signed in (Clerk, or a legacy `csat_` token) → the connected card. Signed
 * out → `requireCredential_` throws the AuthorizationException and Gmail shows
 * the sign-in card. Clerk client id not configured yet (and no legacy token)
 * → the setup card.
 */
function entryCard_(e) {
  var cred = requireCredential_();
  if (!cred) return buildSetupCard_();
  return buildConnectedCard_(e);
}

// ── Widget helpers ──────────────────────────────────────────────────────

/**
 * Row icons come from the web app's own stroke set (1.8px, round caps),
 * rendered to 96px PNGs per hue under `web/public/addin/icons/` and served
 * by the web app — the same host that already serves the logo.
 * `IconImage.setIconUrl` accepts hosted URLs only (no data URIs), so a web
 * deploy must precede any Apps Script push that uses a NEW icon name; an
 * unknown name renders as a broken image, not a fallback glyph.
 *
 * Hues follow the web app's taxonomy: `coral` = the action, `teal` /
 * `plum` / `pink` / `amber` = the kinds of knowledge, `ink` = neutral,
 * `ok` = success, `muted` = disabled.
 */
function icon_(name, hue) {
  return CardService.newIconImage()
    .setIconUrl(
      webBaseUrl_() + '/addin/icons/' + name + '--' + (hue || 'ink') + '.png?v=' + ICON_VERSION,
    )
    .setAltText(name);
}

/** A hosted full-width illustration (the empty states). The alt text is the
 *  whole sentence the image paints, for screen readers and for anyone whose
 *  image proxy fails. */
function illustration_(name, altText) {
  return CardService.newImage()
    .setImageUrl(webBaseUrl_() + '/addin/' + name + '.png')
    .setAltText(altText);
}

/** One `DecoratedText` row. `opts.top` / `opts.bottom` are optional labels;
 *  `opts.text` must already be escaped. `opts.hue` picks the icon colour. */
function row_(iconName, opts) {
  var widget = CardService.newDecoratedText()
    .setStartIcon(icon_(iconName, opts.hue))
    .setWrapText(true);
  if (opts.top) widget.setTopLabel(opts.top);
  widget.setText(opts.text);
  if (opts.bottom) widget.setBottomLabel(opts.bottom);
  return widget;
}

/**
 * An `Action` for `functionName`, optionally carrying `params` — a flat
 * object of STRING values (CardService rejects anything else), read back
 * by the handler as `e.parameters`. This is how a card tells its own
 * handler something the form input cannot (see `PARAM_SAVE_OPTIONS`).
 */
function action_(functionName, params) {
  var action = CardService.newAction().setFunctionName(functionName);
  if (params) action.setParameters(params);
  return action;
}

/**
 * Keep's "Take a note" row: a coral "+" and the action's name, tappable
 * across its whole width. Same handler as the footer button, so the two are
 * never out of step. Pass `muted: true` (no handler) when there is nothing
 * to act on yet — the homepage's "open a message" hint. `params` (optional)
 * ride on the action — see `action_`.
 */
function plusRow_(text, functionName, muted, params) {
  var widget = CardService.newDecoratedText()
    .setStartIcon(icon_('plus', muted ? 'muted' : 'coral'))
    .setText(muted ? '<font color="#8a9099">' + text + '</font>' : text);
  if (!muted && functionName) {
    widget.setOnClickAction(action_(functionName, params));
  }
  return widget;
}

function filledButton_(text, functionName, params) {
  return CardService.newTextButton()
    .setText(text)
    .setTextButtonStyle(CardService.TextButtonStyle.FILLED)
    .setBackgroundColor(BRAND_COLOR)
    .setOnClickAction(action_(functionName, params));
}

function textButton_(text, functionName, params) {
  return CardService.newTextButton()
    .setText(text)
    .setTextButtonStyle(CardService.TextButtonStyle.TEXT)
    .setOnClickAction(action_(functionName, params));
}

/**
 * The two save options on the message card, rendered under the thread row
 * whenever the thread is NOT already followed (`buildConnectedCard_`).
 * Always visible, fixed defaults, nothing remembered between opens:
 *
 * * `scope` — a radio: "Whole thread · N messages" (`thread`, the default;
 *   N from the same `currentMessageSummary_` count the thread row shows,
 *   dropped when unknown) or "Just this message" (`message`).
 * * `follow` — "Keep saving new replies", a switch defaulting to ON. Static
 *   here: it is read at save time, never written on change.
 *
 * `saveToCoralSwarm` reads both from `e.formInput` — but only when the
 * Save action carries `PARAM_SAVE_OPTIONS`, which every caller of this
 * helper must also put on the Save action (see that constant's comment).
 */
function saveOptionsWidgets_(summary, includeFollow) {
  var count = summary && summary.count;
  var wholeThread = count
    ? 'Whole thread · ' + count + (count === 1 ? ' message' : ' messages')
    : 'Whole thread';
  var scope = CardService.newSelectionInput()
    .setType(CardService.SelectionInputType.RADIO_BUTTON)
    .setFieldName('scope')
    .addItem(wholeThread, 'thread', true)
    .addItem('Just this message', 'message', false);
  return includeFollow === false ? [scope] : [scope, followRow_(followSwitch_(true))];
}

/**
 * The `follow` switch itself — field name `follow`, value `'on'` — the ONE
 * place those two strings live, because two handlers read them back
 * (`saveToCoralSwarm` and `toggleFollow`, both via `e.formInput.follow`).
 * Static unless the caller binds an `onChange`.
 */
function followSwitch_(selected) {
  return CardService.newSwitch().setFieldName('follow').setValue('on').setSelected(selected);
}

/** The "Keep saving new replies" row around a `followSwitch_` — the same
 *  icon, hue and copy whether the switch is static or live. */
function followRow_(switchControl) {
  return row_('sync', { hue: 'teal', text: 'Keep saving new replies' }).setSwitchControl(
    switchControl,
  );
}

/**
 * The LIVE follow row on an already-saved thread: the same `followRow_` /
 * `followSwitch_` as the static one in `saveOptionsWidgets_`, but bound to
 * `toggleFollow` (Ingest.gs), which writes `PUT /v1/connectors/google/
 * watched/{threadId}` on every flip and re-renders from the answer. The
 * thread id rides on the action as a parameter so the handler never has
 * to re-resolve it.
 */
function followSwitchRow_(threadId, following) {
  return followRow_(
    followSwitch_(following).setOnChangeAction(action_('toggleFollow', { thread_id: threadId })),
  );
}

function signOutCardAction_() {
  return CardService.newCardAction()
    .setText('Sign out')
    .setOnClickAction(CardService.newAction().setFunctionName('signOut'));
}

/**
 * The account block shared by the message card and the homepage: who/where
 * this mailbox saves to, and how to change it.
 *
 * * Clerk: the workspace (`workspaceName_`: the backend's name for it, else
 *   `workspaceLabel_`: org name if the token has one, else the org id, else
 *   "Personal") plus "Change workspace" and "Sign out".
 * * Legacy `csat_` token: the paired account's email (when known) marked as a
 *   legacy connection, plus a "Sign in with CoralSwarm" button — the migration
 *   path — and "Sign out".
 */
function accountWidgets_() {
  var cred = currentCredential_();
  var widgets = [];
  if (cred && cred.kind === 'clerk') {
    widgets.push(
      row_('account', {
        top: 'Workspace',
        text: escapeHtml_(workspaceName_(cred)),
        bottom: 'Signed in',
      }),
    );
    widgets.push(
      CardService.newButtonSet()
        .addButton(textButton_('Change workspace', 'changeWorkspace'))
        .addButton(textButton_('Sign out', 'signOut')),
    );
    return widgets;
  }
  var email = PropertiesService.getUserProperties().getProperty(PROP_ADDIN_ACCOUNT_EMAIL);
  widgets.push(
    row_('account', {
      text: email ? escapeHtml_(email) : 'Connected',
      bottom: isClientIdConfigured_() ? 'Legacy connection — sign in to upgrade' : 'Legacy connection',
    }),
  );
  if (isClientIdConfigured_()) {
    widgets.push(
      CardService.newButtonSet()
        .addButton(signInButton_('Sign in with CoralSwarm'))
        .addButton(textButton_('Sign out', 'signOut')),
    );
  }
  return widgets;
}

/** The Clerk sign-in button: `setAuthorizationAction` opens the authorization
 *  URL in a popup; when the popup closes (Auth.gs `authCallback`) Gmail
 *  reloads the add-on. */
function signInButton_(text) {
  return CardService.newTextButton()
    .setText(text)
    .setTextButtonStyle(CardService.TextButtonStyle.FILLED)
    .setBackgroundColor(BRAND_COLOR)
    .setAuthorizationAction(
      CardService.newAuthorizationAction().setAuthorizationUrl(
        getClerkService_().getAuthorizationUrl(),
      ),
    );
}

// ── Card builders ───────────────────────────────────────────────────────

/**
 * The sign-in card (Gmail's "custom authorization card", reached through
 * `createSignInUi` in Auth.gs and from every action that finds the user
 * signed out).
 */
function buildSignInCard_() {
  if (!isClientIdConfigured_()) return buildSetupCard_();
  var hero = CardService.newCardSection().addWidget(
    illustration_(
      'empty-start',
      'Your inbox, in your workspace. Save any thread with one tap. Replies follow on their own.',
    ),
  );
  var why = CardService.newCardSection()
    .addWidget(
      row_('bookmark', {
        hue: 'coral',
        text: 'Save any thread',
        bottom: 'The whole conversation, one tap.',
      }),
    )
    .addWidget(row_('sync', { hue: 'teal', text: 'Replies follow automatically' }))
    .addWidget(
      row_('lock', {
        hue: 'plum',
        text: 'Sign in once',
        bottom: 'Pick your workspace in your browser.',
      }),
    );
  var signIn = CardService.newCardSection().addWidget(
    CardService.newButtonSet().addButton(signInButton_('Sign in to CoralSwarm')),
  );
  var lastErr = PropertiesService.getUserProperties().getProperty(PROP_LAST_AUTH_ERROR);
  if (lastErr) {
    signIn.addWidget(
      CardService.newTextParagraph().setText('Your last session could not be renewed, so please sign in again.'),
    );
  }
  return CardService.newCardBuilder()
    .addSection(hero)
    .addSection(why)
    .addSection(signIn)
    .build();
}

/** Shown only while `CLERK_CLIENT_ID` (Config.gs) is still the placeholder and
 *  there is no legacy token to fall back on. */
function buildSetupCard_() {
  return CardService.newCardBuilder()
    .setHeader(CardService.newCardHeader().setTitle('CoralSwarm').setSubtitle('Setup needed'))
    .addSection(
      CardService.newCardSection().addWidget(
        CardService.newTextParagraph().setText(
          'Sign-in is not configured for this add-on yet. Set CLERK_CLIENT_ID in Config.gs ' +
            '(or the CLERK_CLIENT_ID script property) and register this redirect URI on the ' +
            'Clerk OAuth app: ' + escapeHtml_(redirectUri_()),
        ),
      ),
    )
    .build();
}

/**
 * The connected card, for both entry points and for every action that
 * lands back on it. `e` is the trigger/action event: when it carries an
 * open message (`e.gmail.messageId`) the card is the save surface; on the
 * homepage (no message) it is the mailbox's own page — empty state,
 * whole-mailbox switch, account row.
 *
 * The save surface has two shapes, and which one renders is decided ONCE
 * per open by a single `GET …/watched/{threadId}` (`threadFollowing_`):
 *
 * * **Not followed** (the GET said `false`, or could not be read — never
 *   saved, saved-then-unfollowed, network trouble, all alike): "+" row,
 *   thread row, then the save options (`saveOptionsWidgets_`: `scope`
 *   radio, static `follow` switch). The Save action carries
 *   `PARAM_SAVE_OPTIONS = 'shown'`, so `saveToCoralSwarm` sends both.
 * * **Saved** (`saved` passed by an action, or the GET said `true`): the
 *   "In your ocean" row plus the LIVE follow switch (`followSwitchRow_`);
 *   no scope radio. The Save action carries `'follow_only'`: a re-save
 *   sends `follow` = the switch as it stands and no `scope` — a field is
 *   sent iff its control is rendered — so tapping Save after turning the
 *   switch off does NOT silently re-follow (the backend dedupes the
 *   messages). The Save button itself stays; there is no remove action
 *   on this card.
 *
 * `saved` — optional `{ ingested, following }` from `saveToCoralSwarm` /
 * `toggleFollow`; `ingested` is the count the row reports (0 or absent
 * means no count), `following` drives the row copy and the switch and
 * defaults to `true`.
 */
function buildConnectedCard_(e, saved) {
  var messageId = e && e.gmail && e.gmail.messageId;
  if (!messageId) return buildHomepageConnectedCard_();

  // Organization-workspace tokens cannot use the watched routes (403), so the
  // follow switch and the follow read are left out entirely for them.
  var watched = watchedAvailable_();
  var summary = currentMessageSummary_(e);
  var subject = escapeHtml_(summary ? summary.subject : 'This thread');
  var threadId = currentThreadId_(e, summary);

  // Card open (no action told us the state): one cheap read. Only an
  // explicit `true` flips the shape — `false` and `null` (unreadable) both
  // fall through to the ordinary save card, so this call can never break
  // the card.
  if (!saved && watched && threadId && threadFollowing_(threadId) === true) {
    saved = { ingested: 0, following: true };
  }

  var builder = CardService.newCardBuilder().addCardAction(signOutCardAction_());
  var saveParams = null;

  if (saved) {
    var n = saved.ingested || 0;
    var following = saved.following !== false;
    var savedBottom =
      'In your workspace' +
      (following ? ' · following' : '') +
      (n >= 1 ? ' · ' + n + (n === 1 ? ' message saved' : ' messages saved') : '');
    var savedSection = CardService.newCardSection().addWidget(
      row_('check', { hue: 'ok', text: subject, bottom: savedBottom })
        .setEndIcon(icon_('open', 'ink'))
        .setOpenLink(CardService.newOpenLink().setUrl(webBaseUrl_() + '/today')),
    );
    // The live switch needs the thread id to write back; without one there
    // is nothing it could do, so it is left out rather than rendered inert
    // — and then no marker either: nothing rendered, nothing sent.
    if (threadId && watched) {
      savedSection.addWidget(followSwitchRow_(threadId, following));
      saveParams = {};
      saveParams[PARAM_SAVE_OPTIONS] = 'follow_only';
    }
    builder.addSection(savedSection);
  } else {
    saveParams = {};
    saveParams[PARAM_SAVE_OPTIONS] = watched ? 'shown' : 'scope_only';
    var meta = [];
    if (summary && summary.count) {
      meta.push(summary.count + (summary.count === 1 ? ' message' : ' messages'));
    }
    if (summary && summary.from) meta.push(summary.from);
    var saveSection = CardService.newCardSection()
      .addWidget(plusRow_('Save this thread to your workspace', 'saveToCoralSwarm', false, saveParams))
      .addWidget(
        row_('thread', {
          text: subject,
          bottom: meta.length
            ? escapeHtml_(meta.join(' · '))
            : 'The whole conversation, and replies follow automatically.',
        }),
      );
    var options = saveOptionsWidgets_(summary, watched);
    for (var i = 0; i < options.length; i++) saveSection.addWidget(options[i]);
    builder.addSection(saveSection);
  }

  var account = CardService.newCardSection();
  var accountWidgets = accountWidgets_();
  for (var j = 0; j < accountWidgets.length; j++) account.addWidget(accountWidgets[j]);
  builder.addSection(account);
  builder.setFixedFooter(
    CardService.newFixedFooter().setPrimaryButton(
      filledButton_('Save to CoralSwarm', 'saveToCoralSwarm', saveParams),
    ),
  );
  return builder.build();
}

/**
 * Pushed when a save hits the backend's "google is not connected" 404
 * (`isGoogleNotConnected_`, Ingest.gs): the signed-in workspace has no
 * Google connector, so nothing this panel does can make a save land —
 * connecting Gmail is a web-app flow. One row that says so and a button
 * that opens the connectors page. Reuses the `inbox` icon the homepage
 * already hosts (a NEW icon name would need a web deploy first — see
 * `icon_`), and `/connectors` is under the manifest's existing
 * `openLinkUrlPrefixes` entry for the web app.
 */
function buildConnectGmailCard_() {
  return CardService.newCardBuilder()
    .addSection(
      CardService.newCardSection()
        .addWidget(
          row_('inbox', {
            hue: 'coral',
            text: 'Connect Gmail to this workspace first',
            bottom: 'This workspace has no Gmail connection yet, so saves have nowhere to land.',
          }),
        )
        .addWidget(
          CardService.newButtonSet().addButton(
            CardService.newTextButton()
              .setText('Connect Gmail')
              .setTextButtonStyle(CardService.TextButtonStyle.FILLED)
              .setBackgroundColor(BRAND_COLOR)
              .setOpenLink(CardService.newOpenLink().setUrl(webBaseUrl_() + '/connectors')),
          ),
        ),
    )
    .build();
}

/**
 * The homepage while connected: no message to act on, so this is the
 * mailbox's own page — a muted "+" hint, the empty-state illustration,
 * the whole-mailbox switch (backed by `GET/POST /v1/connectors/google/
 * gmail/everything-mode`; hidden when its state cannot be read), and the
 * account row. Auto-ingest rules join this card in the rules train.
 */
function buildHomepageConnectedCard_() {
  var builder = CardService.newCardBuilder()
    .addCardAction(signOutCardAction_())
    .addSection(
      CardService.newCardSection()
        .addWidget(plusRow_('Open a message to save its thread', null, true))
        .addWidget(
          illustration_(
            'empty-home',
            'Nothing from Gmail yet. Threads you save show up in your workspace and on Today.',
          ),
        ),
    );

  var settings = CardService.newCardSection();
  var everything = everythingModeEnabled_();
  if (everything !== null) {
    settings.addWidget(
      row_('inbox', { hue: 'coral', text: 'Ingest my whole mailbox' }).setSwitchControl(
        CardService.newSwitch()
          .setFieldName('everything_mode')
          .setValue('on')
          .setSelected(everything)
          .setOnChangeAction(CardService.newAction().setFunctionName('toggleEverythingMode')),
      ),
    );
  }
  var accountWidgets = accountWidgets_();
  for (var j = 0; j < accountWidgets.length; j++) settings.addWidget(accountWidgets[j]);
  builder.addSection(settings);
  return builder.build();
}

/**
 * The confirm card pushed when the whole-mailbox switch is turned ON. Says
 * exactly what happens (the contract's everything-mode carve-out, `docs/
 * connectors/CONNECTOR_CONTRACT.md` §7) before anything is written. Turning
 * the switch OFF needs no confirmation and never reaches here.
 */
function buildEverythingModeConfirmCard_() {
  var section = CardService.newCardSection()
    .setHeader('Ingest my whole mailbox')
    .addWidget(
      row_('inbox', {
        hue: 'coral',
        text: 'Every message, including existing mail',
        bottom: 'CoralSwarm pulls your mailbox in the background and keeps it in sync.',
      }),
    )
    .addWidget(
      row_('lock', {
        hue: 'plum',
        text: 'Into your private workspace only',
        bottom: 'Nothing is shared unless you promote it yourself.',
      }),
    )
    .addWidget(
      row_('sync', {
        hue: 'teal',
        text: 'Turn it off any time',
        bottom: 'What is already saved stays; the pull stops.',
      }),
    );
  return CardService.newCardBuilder()
    .addSection(section)
    .setFixedFooter(
      CardService.newFixedFooter()
        .setPrimaryButton(filledButton_('Turn on', 'confirmEverythingMode'))
        .setSecondaryButton(textButton_('Cancel', 'cancelEverythingMode')),
    )
    .build();
}
