/**
 * Mailbox-level settings actions for the homepage card (Card.gs,
 * `buildHomepageConnectedCard_`). Today that is one setting: whole-mailbox
 * ingest ("everything mode"), the contract's one permitted exception to
 * per-thread opt-in — `backend/src/routes/gmail_everything_mode.rs`, read
 * that module's doc comment for the three hard requirements it enforces
 * (owner configures it themselves; private ocean only; sharing only via an
 * explicit promotion). This file only drives its two routes:
 *
 * * `GET  /v1/connectors/google/gmail/everything-mode` → `{enabled}` — read
 *   by Card.gs's `everythingModeEnabled_` on every homepage render.
 * * `POST /v1/connectors/google/gmail/everything-mode {enabled}` — written
 *   here, with the signed-in user's credential as bearer (a Clerk access
 *   token, or a legacy `csat_` PAT — `currentCredential_`, Auth.gs; the route
 *   is caller-scoped: it can only ever change the caller's OWN mailbox).
 *
 * Every function here is a CardService action handler and returns an
 * `ActionResponse`.
 */

/**
 * The switch's `onChange`. Turning it ON pushes a confirm card first (a
 * whole-mailbox pull is the one action on these cards worth a second look);
 * turning it OFF writes immediately — stopping is always safe. The switch's
 * value arrives via the form input named `everything_mode` (`'on'` when
 * selected, absent when not).
 */
function toggleEverythingMode(e) {
  var turningOn =
    e &&
    e.formInput &&
    (e.formInput.everything_mode === 'on' || e.formInput.everything_mode === true);
  if (turningOn) {
    return CardService.newActionResponseBuilder()
      .setNavigation(CardService.newNavigation().pushCard(buildEverythingModeConfirmCard_()))
      .build();
  }
  return writeEverythingMode_(false, 'Whole-mailbox ingest is off. Saved mail stays.');
}

/** "Turn on" on the confirm card. */
function confirmEverythingMode(e) {
  return writeEverythingMode_(true, 'Whole-mailbox ingest is on. Your mailbox is being pulled in.');
}

/** "Cancel" on the confirm card — back to the homepage, switch re-read
 *  from the backend so it shows OFF again. */
function cancelEverythingMode(e) {
  return CardService.newActionResponseBuilder()
    .setNavigation(CardService.newNavigation().updateCard(buildHomepageConnectedCard_()))
    .build();
}

/**
 * POST the new state and re-render the homepage from the backend's answer.
 * On 401 the credential is gone or refused — same recovery as
 * `saveToCoralSwarm`: drop it locally and show the sign-in card. Any other
 * failure leaves the switch where the backend says it is and explains.
 */
function writeEverythingMode_(enabled, successText) {
  var cred = currentCredential_();
  if (!cred) {
    return signedOutResponse_('Not connected — reconnect CoralSwarm first.');
  }

  var resp = callBackend_(
    'post',
    backendBaseUrl_() + '/v1/connectors/google/gmail/everything-mode',
    { enabled: enabled },
    cred.token,
  );

  if (resp.status === 401) {
    // The credential was revoked (or the backend does not accept it) — drop it
    // and show the sign-in card rather than failing on every future click.
    return rejectedCredentialResponse_(cred);
  }

  var text = resp.ok
    ? successText
    : resp.status === 403
      ? 'Whole-mailbox ingest is not available for this workspace.'
      : 'Could not change whole-mailbox ingest (' + resp.status + '). Try again in a moment.';
  return CardService.newActionResponseBuilder()
    .setNavigation(CardService.newNavigation().updateCard(buildHomepageConnectedCard_()))
    .setNotification(CardService.newNotification().setText(text))
    .build();
}
