/**
 * Google Calendar surface: approve a Google Meet meeting for transcript capture.
 *
 * Product rule (founder decision): a Meet transcript is saved only for a meeting
 * the user explicitly approves, and approval lives only where the meeting
 * happens: here, in Calendar, while creating / editing the event (or in a Meet
 * side panel, a separate add-on). Nothing is approved implicitly, and there is no
 * approval screen in the CoralSwarm web app.
 *
 * Triggers (appsscript.json `addOns.calendar`):
 *
 * * `eventOpenTrigger` -> `onCalendarEventOpen`: fires when an event is opened,
 *   including the "create event" page. Needs `currentEventAccess: "READ"` and the
 *   `calendar.addons.current.event.read` scope, or `e.calendar.conferenceData` is
 *   not delivered.
 * * `eventUpdateTrigger` -> `onCalendarEventUpdate`: fires (before save) when the
 *   user adds/removes attendees or adds/switches conferencing. Used so a card that
 *   said "add Google Meet first" refreshes the moment Meet is added.
 * * `homepageTrigger` -> `onCalendarHomepage`: the add-on icon with no event open.
 *
 * Backend (`routes::google_meet`, PR #1137), all under `/v1/connectors/google/meet/`:
 * `POST captures {meeting_code}`, `GET captures`, `DELETE captures/{id}`. A
 * pre-approval covers ONE occurrence (the first conference record in that space
 * ending after the approval) and expires after 30 days.
 */

var MEET_CAPTURES_PATH = '/v1/connectors/google/meet/captures';
var MEET_CODE_RE = /^[a-z]{3}-[a-z]{4}-[a-z]{3}$/;
var MEET_URI_RE = /^https?:\/\/meet\.google\.com\/([a-z]{3}-[a-z]{4}-[a-z]{3})(?:[?#\/]|$)/i;
var CONNECT_MEET_URL_PATH = '/connectors';

// ── Reading the Calendar event object ───────────────────────────────────

/**
 * What the Calendar event object says about Google Meet.
 *
 * Returns `{ state, code }`:
 * * `'meet'`    - a Meet meeting code was found (`code`, lower-case `abc-defg-hij`).
 * * `'pending'` - Meet is attached (solution type `hangoutsMeet`) but the event
 *                 carries no code yet. Calendar generates the link when the event
 *                 is saved, so an unsaved event can be in this state.
 * * `'none'`    - no conference data, or a non-Meet solution.
 *
 * Sources, in order: a `video` entry point's URI (`https://meet.google.com/<code>`),
 * any entry point's `meetingCode`, then `conferenceId` when the solution is Meet.
 * Only data Calendar actually provides is trusted: nothing is guessed.
 */
function meetFromCalendarEvent_(cal) {
  var cd = cal && cal.conferenceData;
  if (!cd) return { state: 'none', code: null };

  var entryPoints = cd.entryPoints || [];
  var i;
  for (i = 0; i < entryPoints.length; i++) {
    var ep = entryPoints[i] || {};
    if (ep.entryPointType && ep.entryPointType !== 'video') continue;
    var m = MEET_URI_RE.exec(String(ep.uri || ''));
    if (m) return { state: 'meet', code: m[1].toLowerCase() };
  }
  var isMeetSolution = !!(
    cd.conferenceSolution &&
    cd.conferenceSolution.key &&
    cd.conferenceSolution.key.type === 'hangoutsMeet'
  );
  for (i = 0; i < entryPoints.length; i++) {
    var code = String((entryPoints[i] || {}).meetingCode || '').toLowerCase();
    if (MEET_CODE_RE.test(code) && /meet\.google\.com/i.test(String(entryPoints[i].uri || 'meet.google.com'))) {
      return { state: 'meet', code: code };
    }
  }
  if (isMeetSolution) {
    var id = String(cd.conferenceId || '').toLowerCase();
    if (MEET_CODE_RE.test(id)) return { state: 'meet', code: id };
    return { state: 'pending', code: null };
  }
  return { state: 'none', code: null };
}

/** Whether the event is an instance of a recurring series (`calendar.recurringEventId`). */
function isRecurringEvent_(cal) {
  return !!(cal && cal.recurringEventId);
}

/** A brand-new event the user has not saved yet has no event id. */
function isUnsavedEvent_(cal) {
  return !cal || !cal.id;
}

/**
 * The event's title (`summary`), for display only.
 *
 * Calendar's add-on event object has NO title field (Google docs, "Event
 * objects" -> Calendar event object), and `calendar.addons.current.event.read`
 * does not unlock one. The only way to read it is the Calendar API, via the
 * Calendar Advanced Service (`Calendar.Events.get`), which needs a Calendar read
 * scope (`calendar.events.readonly`). The call asks for `summary` only.
 *
 * Returns the trimmed title, or `null` when there is none: unsaved event (no
 * `calendar.id`), untitled event, scope not yet granted, API error, service
 * missing. Purely presentational; it never throws and never blocks Approve.
 */
function calendarEventTitle_(cal) {
  if (isUnsavedEvent_(cal)) return null;
  try {
    var ev = Calendar.Events.get(cal.calendarId || 'primary', cal.id, { fields: 'summary' });
    var title = String((ev && ev.summary) || '').replace(/\s+/g, ' ').trim();
    return title ? title : null;
  } catch (err) {
    console.warn('calendarEventTitle_: lookup failed: ' + (err && err.message ? err.message : err));
    return null;
  }
}

/** Title for the card: the one an action carried, else a fresh lookup. */
function cardTitle_(e, cal) {
  var carried = e && e.parameters && e.parameters.title;
  if (carried) return String(carried);
  return calendarEventTitle_(cal);
}

/** Action parameters are small; keep the carried title short. */
function clipTitle_(title) {
  var t = String(title || '');
  return t.length > 120 ? t.slice(0, 119) + '\u2026' : t;
}

// ── Backend calls ───────────────────────────────────────────────────────

/** Open (not expired) capture for `code`, preferring an approved-not-yet-saved
 *  one over an already-saved one. `null` when there is none. */
function findMeetCapture_(captures, code) {
  var now = Date.now();
  var allowed = null;
  var ingested = null;
  for (var i = 0; i < (captures || []).length; i++) {
    var c = captures[i] || {};
    if (String(c.meeting_code || '').toLowerCase() !== code) continue;
    if (c.status === 'allowed') {
      var exp = c.expires_at ? Date.parse(c.expires_at) : NaN;
      if (!isNaN(exp) && exp < now) continue;
      if (!allowed) allowed = c;
    } else if (c.status === 'ingested' && !ingested) {
      ingested = c;
    }
  }
  return { allowed: allowed, ingested: ingested };
}

/** Classify a non-2xx backend answer for the Meet routes. */
function meetErrorKind_(resp) {
  var err = resp && resp.body && resp.body.error;
  if (resp.status === 401) return 'unauthorized';
  if (resp.status === 409 && err === 'already_ingested') return 'already_ingested';
  if (resp.status === 409 || err === 'google_meet_not_connected') return 'not_connected';
  if (resp.status === 403) return 'disabled';
  if (resp.status === 404 && err === 'no_capture') return 'no_capture';
  return 'other';
}

// ── Scope consent ───────────────────────────────────────────────────────

/** Scope the title lookup needs (also listed in appsscript.json `oauthScopes`). */
var CALENDAR_TITLE_SCOPE = 'https://www.googleapis.com/auth/calendar.events.readonly';

/**
 * Make Google ask for the title scope when the user has not granted it.
 *
 * Google does NOT re-prompt an already-installed user for a scope added to the
 * manifest later; `Calendar.Events.get` then fails with "You do not have
 * permission to call calendar.events.get", and the card quietly lost the title.
 * `ScriptApp.requireScopes(FULL, [scope])` validates the grant and, when it is
 * missing, ENDS the current execution and renders Google's authorization prompt
 * (docs: apps-script/reference/script/script-app#requireScopes,
 * apps-script/concepts/scopes). For a Workspace add-on (a surface without
 * granular consent) Google renders the prompt at the start of the execution.
 * Either way the exception must propagate, so never wrap this in try/catch.
 *
 * Call it first in every Calendar TRIGGER, before building any card. Not in the
 * action handlers: Approve/Undo must keep working, and by then the card was
 * either rendered with the grant or the user already saw the prompt.
 * Declining leaves the user with the prompt (no card) until they grant it:
 * accepted, because the founder wants the meeting name shown.
 */
function requireCalendarTitleScope_() {
  if (typeof ScriptApp.requireScopes !== 'function') return;
  ScriptApp.requireScopes(ScriptApp.AuthMode.FULL, [CALENDAR_TITLE_SCOPE]);
}

// ── Entry points ────────────────────────────────────────────────────────

function onCalendarEventOpen(e) {
  return calendarEntryCard_(e);
}

function onCalendarEventUpdate(e) {
  return calendarEntryCard_(e);
}

function onCalendarHomepage(e) {
  requireCalendarTitleScope_();
  var cred = requireCredential_();
  if (!cred) return buildSetupCard_();
  var builder = CardService.newCardBuilder()
    .addCardAction(signOutCardAction_())
    .addSection(
      CardService.newCardSection().addWidget(
        row_('bookmark', {
          hue: 'coral',
          text: 'Open an event to approve its Google Meet transcript',
          bottom: 'Approval happens here, while you create or edit the meeting.',
        }),
      ),
    );
  var account = CardService.newCardSection();
  var widgets = accountWidgets_();
  for (var i = 0; i < widgets.length; i++) account.addWidget(widgets[i]);
  return builder.addSection(account).build();
}

/** Signed out -> sign-in card (via `requireCredential_`); signed in -> the event card. */
function calendarEntryCard_(e) {
  requireCalendarTitleScope_();
  var cred = requireCredential_();
  if (!cred) return buildSetupCard_();
  return buildCalendarEventCard_(e);
}

// ── Cards ───────────────────────────────────────────────────────────────

/**
 * The event card. `state` (optional) is passed by an action that already knows
 * the outcome: `{ capture }` (just approved), `{ cleared: true }` (just undone).
 * Otherwise state is read with one `GET captures` (matched by meeting code), so
 * reopening an approved event shows Approved.
 */
function buildCalendarEventCard_(e, state) {
  var cal = (e && e.calendar) || {};
  var meet = meetFromCalendarEvent_(cal);
  var recurring = isRecurringEvent_(cal);
  var unsaved = isUnsavedEvent_(cal);
  var cred = currentCredential_();
  var builder = CardService.newCardBuilder().addCardAction(signOutCardAction_());
  var title = meet.state === 'meet' ? clipTitle_(cardTitle_(e, cal) || '') : '';

  if (meet.state !== 'meet') {
    builder.addSection(noMeetSection_(meet.state, unsaved));
    return finishCalendarCard_(builder, cred, null);
  }

  var found = { allowed: null, ingested: null };
  if (state && state.capture) {
    found.allowed = state.capture;
  } else if (state && state.cleared) {
    // Just undone: do not re-read (the list can lag); show the prompt.
  } else {
    var resp = callBackend_('get', backendBaseUrl_() + MEET_CAPTURES_PATH, null, cred.token);
    if (resp.ok) {
      found = findMeetCapture_((resp.body && resp.body.captures) || [], meet.code);
    } else {
      var kind = meetErrorKind_(resp);
      if (kind === 'unauthorized') return rejectedCredentialCard_(cred);
      if (kind === 'not_connected') return finishCalendarCard_(builder.addSection(connectMeetSection_()), cred, meet.code);
      if (kind === 'disabled') return finishCalendarCard_(builder.addSection(notEnabledSection_()), cred, meet.code);
      // Any other read failure: fall through to the prompt. The POST reports its
      // own error, so an unreadable state never blocks approving.
    }
  }

  if (found.allowed) {
    builder.addSection(approvedSection_(meet.code, found.allowed, recurring, title));
  } else if (found.ingested && !recurring) {
    builder.addSection(savedSection_(meet.code, title));
  } else {
    builder.addSection(promptSection_(meet.code, recurring, unsaved, !!found.ingested, title));
  }
  return finishCalendarCard_(builder, cred, meet.code);
}

/** The account block (workspace label, change workspace, sign out) closes every card. */
function finishCalendarCard_(builder, cred, code) {
  var account = CardService.newCardSection();
  var widgets = accountWidgets_();
  for (var i = 0; i < widgets.length; i++) account.addWidget(widgets[i]);
  builder.addSection(account);
  return builder.build();
}

function noMeetSection_(state, unsaved) {
  var section = CardService.newCardSection().addWidget(
    row_('bookmark', {
      hue: 'coral',
      text: 'Add Google Meet video conferencing to this event first.',
      bottom:
        state === 'pending' || unsaved
          ? 'If you just added it, save the event, then tap Check again.'
          : 'CoralSwarm saves transcripts of Google Meet meetings you approve.',
    }),
  );
  section.addWidget(
    CardService.newButtonSet().addButton(textButton_('Check again', 'refreshCalendarCard')),
  );
  return section;
}

function promptSection_(code, recurring, unsaved, earlierSaved, title) {
  var bottom = [];
  if (earlierSaved) bottom.push('An earlier occurrence was saved.');
  if (recurring) bottom.push('Approves the next occurrence only.');
  else if (unsaved) bottom.push('If this event repeats, approval covers the next occurrence only.');
  bottom.push('Transcripts must be turned on for the meeting.');
  var params = { meeting_code: code, recurring: recurring ? '1' : '', title: title || '' };
  var heading = title
    ? 'Save "' + escapeHtml_(title) + '" to your CoralSwarm workspace?'
    : 'Save this meeting to your CoralSwarm workspace?';
  return CardService.newCardSection()
    .addWidget(plusRow_(heading, 'approveMeetCapture', false, params))
    .addWidget(row_('thread', { text: 'Meet \u00b7 ' + escapeHtml_(code), bottom: escapeHtml_(bottom.join(' ')) }))
    .addWidget(
      CardService.newButtonSet().addButton(filledButton_('Approve', 'approveMeetCapture', params)),
    );
}

function approvedSection_(code, capture, recurring, title) {
  var params = { meeting_code: code, capture_id: String(capture.id || ''), recurring: recurring ? '1' : '', title: title || '' };
  return CardService.newCardSection()
    .addWidget(
      row_('check', {
        hue: 'ok',
        top: title ? escapeHtml_(title) : 'Meet \u00b7 ' + escapeHtml_(code),
        text: 'Approved',
        bottom:
          (title ? 'Meet \u00b7 ' + escapeHtml_(code) + '. ' : '') +
          'The transcript saves after the meeting (transcripts must be on).' +
          (recurring ? ' Approves the next occurrence only.' : ''),
      }),
    )
    .addWidget(CardService.newButtonSet().addButton(textButton_('Undo', 'undoMeetCapture', params)));
}

function savedSection_(code, title) {
  return CardService.newCardSection().addWidget(
    row_('check', {
      hue: 'ok',
      top: title ? escapeHtml_(title) : 'Meet \u00b7 ' + escapeHtml_(code),
      text: 'Saved to your workspace',
      bottom: (title ? 'Meet \u00b7 ' + escapeHtml_(code) + '. ' : '') + 'This meeting\'s transcript has been saved.',
    })
      .setEndIcon(icon_('open', 'ink'))
      .setOpenLink(CardService.newOpenLink().setUrl(webBaseUrl_() + '/today')),
  );
}

function connectMeetSection_() {
  return CardService.newCardSection()
    .addWidget(
      row_('inbox', {
        hue: 'coral',
        text: 'Connect Google Meet first',
        bottom: 'CoralSwarm reads the transcript with your own Google Meet connection.',
      }),
    )
    .addWidget(
      CardService.newButtonSet().addButton(
        CardService.newTextButton()
          .setText('Connect Google Meet')
          .setTextButtonStyle(CardService.TextButtonStyle.FILLED)
          .setBackgroundColor(BRAND_COLOR)
          .setOpenLink(CardService.newOpenLink().setUrl(webBaseUrl_() + CONNECT_MEET_URL_PATH)),
      ),
    );
}

function notEnabledSection_() {
  return CardService.newCardSection().addWidget(
    row_('lock', {
      hue: 'plum',
      text: 'Google Meet capture is not enabled for this workspace',
      bottom: 'Ask your CoralSwarm admin to turn on the Google Meet connector, or switch workspace.',
    }),
  );
}

/** Credential refused (401) while building a card (not an action): drop it and show sign-in. */
function rejectedCredentialCard_(cred) {
  if (cred && cred.kind === 'clerk') {
    getClerkService_().reset();
  } else {
    retireLegacyToken_(true);
  }
  return buildSignInCard_();
}

// ── Actions ─────────────────────────────────────────────────────────────

function calendarActionResponse_(card, text) {
  var b = CardService.newActionResponseBuilder().setNavigation(CardService.newNavigation().updateCard(card));
  if (text) b.setNotification(CardService.newNotification().setText(text));
  return b.build();
}

/** The action's own event data when Calendar sends it, else the parameters the
 *  card carried, so a handler never depends on `e.calendar` being re-delivered. */
function calendarEventForAction_(e) {
  var p = (e && e.parameters) || {};
  var cal = (e && e.calendar) || {};
  if (!cal.conferenceData && p.meeting_code) {
    cal = {
      calendarId: cal.calendarId,
      id: cal.id,
      recurringEventId: p.recurring === '1' ? cal.recurringEventId || 'recurring' : cal.recurringEventId,
      conferenceData: {
        conferenceSolution: { key: { type: 'hangoutsMeet' } },
        entryPoints: [{ entryPointType: 'video', uri: 'https://meet.google.com/' + p.meeting_code }],
      },
    };
  }
  return { calendar: cal, parameters: p };
}

/** "Check again": re-read the event object and rebuild the card. */
function refreshCalendarCard(e) {
  return calendarActionResponse_(buildCalendarEventCard_(e));
}

/** "Approve": POST captures {meeting_code}. */
function approveMeetCapture(e) {
  var cred = currentCredential_();
  if (!cred) return signedOutResponse_('Not connected. Sign in to CoralSwarm first.');
  var ev = calendarEventForAction_(e);
  var code = String((ev.parameters && ev.parameters.meeting_code) || '').toLowerCase();
  if (!MEET_CODE_RE.test(code)) {
    return calendarActionResponse_(buildCalendarEventCard_(ev), 'Could not read the Meet link. Save the event and try again.');
  }
  var resp = callBackend_('post', backendBaseUrl_() + MEET_CAPTURES_PATH, { meeting_code: code }, cred.token);
  if (resp.ok && resp.body && resp.body.capture) {
    return calendarActionResponse_(buildCalendarEventCard_(ev, { capture: resp.body.capture }), 'Approved.');
  }
  var kind = meetErrorKind_(resp);
  if (kind === 'unauthorized') return rejectedCredentialResponse_(cred);
  var builder = CardService.newCardBuilder().addCardAction(signOutCardAction_());
  if (kind === 'not_connected') builder.addSection(connectMeetSection_());
  else if (kind === 'disabled') builder.addSection(notEnabledSection_());
  else {
    return calendarActionResponse_(
      buildCalendarEventCard_(ev, { cleared: true }),
      'Could not approve this meeting (' + (resp.status || 'network error') + '). Try again in a moment.',
    );
  }
  return calendarActionResponse_(finishCalendarCard_(builder, cred, code));
}

/** "Undo": DELETE captures/{id}. */
function undoMeetCapture(e) {
  var cred = currentCredential_();
  if (!cred) return signedOutResponse_('Not connected. Sign in to CoralSwarm first.');
  var ev = calendarEventForAction_(e);
  var id = String((ev.parameters && ev.parameters.capture_id) || '');
  if (!id) {
    return calendarActionResponse_(buildCalendarEventCard_(ev), 'Nothing to undo.');
  }
  var resp = callBackend_('delete', backendBaseUrl_() + MEET_CAPTURES_PATH + '/' + encodeURIComponent(id), null, cred.token);
  var kind = resp.ok ? 'ok' : meetErrorKind_(resp);
  if (kind === 'unauthorized') return rejectedCredentialResponse_(cred);
  if (kind === 'ok' || kind === 'no_capture') {
    return calendarActionResponse_(buildCalendarEventCard_(ev, { cleared: true }), 'Approval removed.');
  }
  if (kind === 'already_ingested') {
    return calendarActionResponse_(buildCalendarEventCard_(ev), 'This meeting was already saved; undo cannot remove the copy.');
  }
  return calendarActionResponse_(
    buildCalendarEventCard_(ev),
    'Could not undo (' + (resp.status || 'network error') + '). Try again in a moment.',
  );
}
