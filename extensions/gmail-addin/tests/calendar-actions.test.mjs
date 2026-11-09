import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';

// CardService is only available inside Apps Script. This adapter records its
// documented builder/action values; the real card and action handlers run below.
function builder() {
  const value = {};
  for (const name of [
    'setText', 'setTextButtonStyle', 'setBackgroundColor', 'setOnClickAction',
    'setFunctionName', 'setStartIcon', 'setWrapText', 'setTopLabel',
    'setBottomLabel', 'setImageUrl', 'setIconUrl', 'setAltText', 'setNavigation',
    'setNotification',
  ]) {
    value[name] = (input) => { value[name.slice(3)] = input; return value; };
  }
  value.updateCard = (card) => { value.card = card; return value; };
  for (const name of ['addWidget', 'addButton', 'addSection', 'addCardAction']) {
    value[name] = (input) => { (value[`${name}Values`] ??= []).push(input); return value; };
  }
  value.setParameters = (parameters) => {
    assert.ok(Object.values(parameters).every((v) => typeof v === 'string'));
    value.parameters = { ...parameters };
    return value;
  };
  value.build = () => value;
  return value;
}

function harness() {
  const calls = [];
  const context = vm.createContext({
    console,
    CardService: {
      TextButtonStyle: { TEXT: 'TEXT', FILLED: 'FILLED' },
      ...Object.fromEntries([
        'newCardBuilder', 'newCardSection', 'newButtonSet', 'newTextButton',
        'newAction', 'newDecoratedText', 'newIconImage', 'newNavigation',
        'newActionResponseBuilder', 'newNotification',
      ].map((name) => [name, builder])),
    },
  });
  for (const file of ['Card.gs', 'Calendar.gs']) {
    vm.runInContext(readFileSync(new URL(`../${file}`, import.meta.url), 'utf8'), context, { filename: file });
  }
  Object.assign(context, {
    currentCredential_: () => ({ token: 'synthetic-test-token' }),
    backendBaseUrl_: () => 'https://api.example.test',
    webBaseUrl_: () => 'https://example.test',
    signOutCardAction_: () => ({}),
    accountWidgets_: () => [],
    callBackend_: (method, url, body) => {
      calls.push({ method, url, body });
      return context.reply(method);
    },
  });
  return { context, calls };
}

// POST pre_approve returns {capture: Capture}; Capture serializes its UUID as
// `id` (backend/src/routes/google_meet.rs and services/google_meet_capture.rs).
const capture = { id: 'e8a7fabe-09a3-4d29-aa3f-19d27abdc55e', meeting_code: 'abc-defg-hij', status: 'allowed' };
const parameters = { meeting_code: capture.meeting_code, recurring: '', title: 'Synthetic meeting' };

function undoAction(response) {
  const card = response.Navigation.card;
  return card.addSectionValues
    .flatMap((section) => section.addWidgetValues || [])
    .flatMap((widget) => widget.addButtonValues || [])
    .find((button) => button.Text === 'Undo').OnClickAction;
}

for (const status of [201, 200]) {
  test(`approval ${status} renders an Undo action that revokes only its returned capture`, () => {
    const { context, calls } = harness();
    context.reply = (method) => method === 'post'
      ? { ok: true, status, body: { capture, capture_state: 'allowed', created: status === 201 } }
      : { ok: true, status: 200, body: { revoked: true } };
    const approved = context.approveMeetCapture({ parameters });
    assert.equal(approved.Notification.Text, 'Approved.');
    const action = undoAction(approved);
    assert.equal(action.FunctionName, 'undoMeetCapture');
    assert.equal(action.parameters.capture_id, capture.id);
    const undone = context[action.FunctionName]({ parameters: action.parameters });
    assert.equal(undone.Notification.Text, 'Approval removed.');
    assert.deepEqual(calls.map(({ method, url }) => ({ method, url })), [
      { method: 'post', url: 'https://api.example.test/v1/connectors/google/meet/captures' },
      { method: 'delete', url: `https://api.example.test/v1/connectors/google/meet/captures/${capture.id}` },
    ]);
    assert.equal(calls[0].body.meeting_code, capture.meeting_code);
    assert.equal(calls[1].body, null);
  });
}

test('reopening an approved event preserves the capture-specific Undo action', () => {
  const { context, calls } = harness();
  context.reply = (method) => ({ ok: true, status: 200, body: method === 'get' ? { captures: [capture] } : { revoked: true } });
  const event = context.calendarEventForAction_({ parameters });
  const action = undoAction(context.calendarActionResponse_(context.buildCalendarEventCard_(event)));
  context.undoMeetCapture({ parameters: action.parameters });
  assert.deepEqual(calls.map((call) => call.method), ['get', 'delete']);
  assert.equal(calls[1].url.split('/').at(-1), capture.id);
});

test('a missing capture ID never guesses or issues a revoke request', () => {
  const { context, calls } = harness();
  context.reply = () => ({ ok: true, status: 200, body: { captures: [capture] } });
  const response = context.undoMeetCapture({ parameters });
  assert.equal(response.Notification.Text, 'Nothing to undo.');
  assert.ok(calls.every((call) => call.method === 'get'));
});

test('existing parameterless Check again button still invokes refresh without parameters', () => {
  const { context } = harness();
  const section = context.noMeetSection_('missing', false);
  const action = section.addWidgetValues.at(-1).addButtonValues[0].OnClickAction;
  assert.equal(action.FunctionName, 'refreshCalendarCard');
  assert.equal(action.parameters, undefined);
});
