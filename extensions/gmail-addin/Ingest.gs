/**
 * The connected-state actions on the message card: opt the open message
 * (or its whole thread) into CoralSwarm, and flip reply auto-capture on a
 * thread that is already in. Mirrors the Outlook add-in's "Save to CoralSwarm"
 * (web/components/addin/EmailPane.tsx) at the wire level — see backend/src/routes/connectors.rs's
 * `ingest_selected` doc comment: only the open message's id is ever sent,
 * never the mailbox; the server expands it to the full conversation
 * server-side (unless told `scope: "message"`) and registers it for reply
 * auto-capture (unless told `follow: false`).
 *
 * Every function here is a CardService action handler and returns an
 * `ActionResponse`.
 */

/**
 * `POST /v1/connectors/google/ingest`. The body is always
 * `{item_ids: [messageId]}`; a save option rides along iff the card that
 * fired this action RENDERED its control — the same rule as the Outlook
 * add-in — signalled by the `PARAM_SAVE_OPTIONS` action parameter
 * (Card.gs), never inferred from the form input:
 *
 * * `'shown'` (the save card) → `scope` is the radio's value (`"thread"`
 *   unless the user picked `"message"`) and `follow` is `true` iff the
 *   switch is on — an unchecked switch is simply ABSENT from
 *   `e.formInput`, so absence here means off, not unknown.
 * * `'follow_only'` (the saved card: live switch, no radio) → `follow` as
 *   the switch stands, no `scope`. So a re-save after turning the switch
 *   off sends `follow: false` and does not silently re-follow; the backend
 *   dedupes the messages either way.
 * * absent → neither field; the backend applies its defaults (`thread`,
 *   `true`). No card renders this today — kept as the safe fallback.
 *
 * The re-rendered card's `following` is what was sent (the backend sets
 * `follow_replies` to the request's value on conflict), or the default
 * `true` when nothing was, so the card is truthful in every state.
 */
function saveToCoralSwarm(e) {
  var cred = currentCredential_();
  if (!cred) {
    return signedOutResponse_('Not connected — reconnect CoralSwarm first.');
  }

  var messageId = e && e.gmail && e.gmail.messageId;
  if (!messageId) {
    return CardService.newActionResponseBuilder()
      .setNotification(CardService.newNotification().setText('No message is open.'))
      .build();
  }

  var rendered = (e && e.parameters && e.parameters[PARAM_SAVE_OPTIONS]) || '';
  var scopeRendered = rendered === 'shown' || rendered === 'scope_only';
  var followRendered = rendered === 'shown' || rendered === 'follow_only';
  var input = (e && e.formInput) || {};
  var body = { item_ids: [messageId] };
  if (scopeRendered) body.scope = input.scope === 'message' ? 'message' : 'thread';
  // What the re-rendered card says about following: what we sent, else
  // the backend's default (`true`).
  // Without the follow control (org workspace) the card must not claim following.
  var following = watchedAvailable_();
  if (followRendered) {
    following = input.follow === 'on' || input.follow === true;
    body.follow = following;
  }

  var resp = callBackend_('post', backendBaseUrl_() + '/v1/connectors/google/ingest', body, cred.token);

  if (resp.status === 401) {
    // The credential was revoked (or the backend does not accept it) — drop it
    // and show the sign-in card rather than failing on every future click.
    return rejectedCredentialResponse_(cred);
  }
  if (isGoogleNotConnected_(resp)) {
    // The chosen workspace has no Google connector, so no save can land
    // there until Gmail is connected — a web-app flow, not a retry. Push the
    // card that says so and links the connectors page instead of toasting a
    // bare status code.
    return CardService.newActionResponseBuilder()
      .setNavigation(CardService.newNavigation().pushCard(buildConnectGmailCard_()))
      .setNotification(
        CardService.newNotification().setText('Connect Gmail to this workspace first'),
      )
      .build();
  }
  if (!resp.ok) {
    return CardService.newActionResponseBuilder()
      .setNotification(
        CardService.newNotification().setText('Could not save this thread (' + resp.status + ').'),
      )
      .build();
  }

  var ingested = resp.body && typeof resp.body.ingested === 'number' ? resp.body.ingested : 0;
  // Re-render the connected card with the thread row in its saved state
  // (Card.gs, `buildConnectedCard_`), and say how much actually landed —
  // `ingested` is the count of messages the backend newly stored, so a
  // thread that was already in the ocean reports 0 here, not an error.
  return CardService.newActionResponseBuilder()
    .setNavigation(
      CardService.newNavigation().updateCard(
        buildConnectedCard_(e, { ingested: ingested, following: following }),
      ),
    )
    .setNotification(
      CardService.newNotification().setText(
        ingested >= 1
          ? 'Saved to CoralSwarm · ' + ingested + (ingested === 1 ? ' message' : ' messages')
          : 'Already in your workspace — nothing new to save.',
      ),
    )
    .build();
}

/**
 * The live follow switch's `onChange` on an already-saved thread (Card.gs,
 * `followSwitchRow_`): `PUT /v1/connectors/google/watched/{threadId}
 * {follow}` → `{following}`, then the card is re-rendered from the answer
 * — never from what was asked for. The thread id arrives as the action
 * parameter `thread_id` (with `e.gmail.threadId` as the fallback); the new
 * state is the switch's form input, `'on'` when selected and absent when
 * not.
 *
 * Outcomes: 200 → saved card with the switch where the backend says it is,
 * toast "Stopped following" / "Following again" — and because that card's
 * Save action carries `'follow_only'`, a Save tapped afterwards sends the
 * switch's state (`saveToCoralSwarm`), so an unfollowed thread stays
 * unfollowed through a re-save. 404 (no watched row for
 * this thread — it was never saved; every save writes the row, a
 * `follow: false` one included, so this is only ever a stale card) → say
 * so and fall back to the ordinary save card. 401 → the token is gone,
 * same recovery as `saveToCoralSwarm`. Anything else → the switch snaps
 * back to its previous state and the toast explains, so the card never
 * shows a state the backend didn't confirm.
 */
function toggleFollow(e) {
  var cred = currentCredential_();
  if (!cred) {
    return signedOutResponse_('Not connected — reconnect CoralSwarm first.');
  }

  var threadId =
    (e && e.parameters && e.parameters.thread_id) || currentThreadId_(e, null);
  if (!threadId) {
    return CardService.newActionResponseBuilder()
      .setNotification(CardService.newNotification().setText('No thread is open.'))
      .build();
  }

  var input = (e && e.formInput) || {};
  var follow = input.follow === 'on' || input.follow === true;

  var resp = callBackend_(
    'put',
    backendBaseUrl_() + '/v1/connectors/google/watched/' + encodeURIComponent(threadId),
    { follow: follow },
    cred.token,
  );

  if (resp.status === 401) {
    // The credential was revoked (or the backend does not accept it) — drop it
    // and show the sign-in card rather than failing on every future click.
    return rejectedCredentialResponse_(cred);
  }
  if (resp.status === 404) {
    // No watched row to flip. Re-render without `saved`: the card re-reads
    // the backend (which will say not following) and shows the save
    // options, which is the honest next step.
    return CardService.newActionResponseBuilder()
      .setNavigation(CardService.newNavigation().updateCard(buildConnectedCard_(e)))
      .setNotification(
        CardService.newNotification().setText('This thread is not in your workspace yet — save it first.'),
      )
      .build();
  }
  if (!resp.ok || !resp.body || typeof resp.body.following !== 'boolean') {
    return CardService.newActionResponseBuilder()
      .setNavigation(
        CardService.newNavigation().updateCard(
          buildConnectedCard_(e, { ingested: 0, following: !follow }),
        ),
      )
      .setNotification(
        CardService.newNotification().setText(
          'Could not change following (' + resp.status + '). Try again in a moment.',
        ),
      )
      .build();
  }

  var following = resp.body.following;
  return CardService.newActionResponseBuilder()
    .setNavigation(
      CardService.newNavigation().updateCard(
        buildConnectedCard_(e, { ingested: 0, following: following }),
      ),
    )
    .setNotification(
      CardService.newNotification().setText(following ? 'Following again' : 'Stopped following'),
    )
    .build();
}

/**
 * Whether a `callBackend_` answer is the backend's "google is not
 * connected" 404 — the signed-in workspace has no Google connector
 * (`backend/src/routes/connectors.rs` answers `404 {"error":"google is not
 * connected"}` when the connector row is absent). Matched on the error
 * TEXT as well as the status: any other 404 keeps the generic failure
 * toast rather than claiming a missing connector it knows nothing about.
 */
function isGoogleNotConnected_(resp) {
  return (
    resp.status === 404 &&
    !!resp.body &&
    typeof resp.body.error === 'string' &&
    resp.body.error.indexOf('not connected') !== -1
  );
}
