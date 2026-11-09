// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { OutlookComposePane, COMPOSE_POLL_MS, COMPOSE_STATUS_TIMEOUT_MS } from "./OutlookComposePane";
import { Surface } from "./kit";
import type { ConnectedContext } from "./AddinApp";
import type { AddinClient, Workspace } from "@/lib/addin/microsoftSso";
import type { OfficeApi, OfficeMailItem } from "@/lib/addin/office";
import { ApiError } from "@/lib/api";
import { NONCE_PROPERTY, INTENT_ID_PROPERTY, EXPIRES_PROPERTY } from "@/lib/addin/composeIntent";

const calls = vi.hoisted(() => ({ create: vi.fn(), status: vi.fn(), cancel: vi.fn(), byNonce: vi.fn(), connect: vi.fn() }));
vi.mock("@/lib/api", async () => ({
  ...(await vi.importActual<typeof import("@/lib/api")>("@/lib/api")),
  api: { outlookCaptureSchedule: calls, msTeams: { connectMeeting: calls.connect } },
}));

const NONCE = "11111111-1111-4111-a111-111111111111";
const INTENT = "22222222-2222-4222-a222-222222222222";
const org: Workspace = { account_id: "acct-o", name: "Acme ISD", kind: "org" };
const personal: Workspace = { account_id: "acct-p", name: "Personal", kind: "personal" };
const idle = { status: "pending_save", error_code: null, recurring: false, next_occurrence_start: null, auto_transcription: null, manual_reason: null };

function draft(opts: { data?: Record<string, string>; customProps?: boolean; recurrence?: unknown; saveStatus?: string } = {}) {
  const data: Record<string, string> = { ...(opts.data ?? {}) };
  const props = {
    get: (k: string) => data[k], set: (k: string, v: string) => { data[k] = v; }, remove: (k: string) => { delete data[k]; },
    saveAsync: vi.fn((cb: (r: { status: string; value: unknown }) => void) => cb({ status: opts.saveStatus ?? "ok", value: undefined })),
  };
  const itemSave = vi.fn();
  const item = {
    itemType: "appointment", subject: { getAsync() {} }, saveAsync: itemSave,
    ...(opts.customProps === false ? {} : { loadCustomPropertiesAsync: (cb: (r: { status: string; value: unknown }) => void) => cb({ status: "ok", value: props }) }),
    ...(opts.recurrence === undefined ? {} : { recurrence: { getAsync: (cb: (r: { status: string; value: unknown }) => void) => cb({ status: "ok", value: opts.recurrence }) } }),
  } as unknown as OfficeMailItem;
  const office = { AsyncResultStatus: { Succeeded: "ok" }, context: { mailbox: { item } } } as unknown as OfficeApi;
  return { data, props, itemSave, office };
}

function setup(d = draft(), workspace: Workspace = org, tokenImpl?: (fn: (token: string) => Promise<unknown>) => Promise<unknown>) {
  const openAuthentication = vi.fn(async () => {});
  const reconnectRequired = vi.fn(); const openWorkspacePicker = vi.fn();
  const withToken = vi.fn(tokenImpl ?? (async (fn: (token: string) => Promise<unknown>) => fn("session-token")));
  const context = {
    session: { token: "session-token", expiresAt: Date.now() + 3600_000, account: { email: "ada@acme.test" }, workspace, autoLinked: false },
    client: { withToken } as unknown as AddinClient, toast: vi.fn(), reconnectRequired, openWorkspacePicker,
  } as ConnectedContext;
  const view = render(<Surface label="test"><OutlookComposePane context={context} office={d.office} openAuthentication={openAuthentication} /></Surface>);
  return { ...view, ...d, openWorkspacePicker, openAuthentication };
}

beforeEach(() => {
  vi.clearAllMocks();
  calls.create.mockResolvedValue({ intent_id: INTENT, nonce: NONCE, expires_at: "2099-01-01T00:00:00Z" });
  calls.status.mockResolvedValue(idle);
  calls.cancel.mockResolvedValue(undefined);
});
afterEach(() => { cleanup(); vi.useRealTimers(); });

describe("turning it on and off", () => {
  it("offers 'Save this meeting to CoralSwarm' and the Meeting-options recording prompt BEFORE it is on", async () => {
    setup();
    expect(await screen.findByRole("button", { name: "Save this meeting to CoralSwarm" })).toBeVisible();
    expect(screen.getByTestId("transcribe-prompt")).toHaveTextContent("Turn on Record and transcribe automatically in Teams Meeting options.");
    expect(calls.create).not.toHaveBeenCalled(); // opening writes nothing
  });

  it("creates an intent, writes the nonce to the draft's properties (never item.saveAsync), and shows On", async () => {
    const t = setup();
    fireEvent.click(await screen.findByRole("button", { name: "Save this meeting to CoralSwarm" }));
    expect(await screen.findByText("On — CoralSwarm saves the transcript after you save or send this meeting.")).toBeVisible();
    expect(calls.create).toHaveBeenCalledWith("session-token");
    expect(t.data[NONCE_PROPERTY]).toBe(NONCE); expect(t.data[INTENT_ID_PROPERTY]).toBe(INTENT);
    expect(t.props.saveAsync).toHaveBeenCalledTimes(1);
    expect(t.itemSave).not.toHaveBeenCalled();
    expect(await screen.findByTestId("transcribe-prompt")).toBeVisible();
    await vi.waitFor(() => expect(calls.status).toHaveBeenCalledWith("session-token", INTENT, expect.anything()));
  });

  it("turning it off DELETEs the intent and removes the marker", async () => {
    const t = setup(draft({ data: { [NONCE_PROPERTY]: NONCE, [INTENT_ID_PROPERTY]: INTENT, other: "kept" } }));
    fireEvent.click(await screen.findByRole("button", { name: "Turn off" }));
    expect(await screen.findByRole("button", { name: "Save this meeting to CoralSwarm" })).toBeVisible();
    expect(calls.cancel).toHaveBeenCalledWith("session-token", INTENT);
    expect(t.data[NONCE_PROPERTY]).toBeUndefined(); expect(t.data.other).toBe("kept");
  });

  it("reopening the same draft shows On from the stored marker without creating a new intent", async () => {
    setup(draft({ data: { [NONCE_PROPERTY]: NONCE, [INTENT_ID_PROPERTY]: INTENT } }));
    expect(await screen.findByText(/^On — CoralSwarm saves the transcript after you save/)).toBeVisible();
    expect(calls.create).not.toHaveBeenCalled();
    expect(screen.queryByRole("button", { name: "Save this meeting to CoralSwarm" })).not.toBeInTheDocument();
  });

  it("if Outlook refuses the marker, the intent is cancelled and the user can retry", async () => {
    setup(draft({ saveStatus: "failed" }));
    fireEvent.click(await screen.findByRole("button", { name: "Save this meeting to CoralSwarm" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Couldn’t turn this on. Try again.");
    await vi.waitFor(() => expect(calls.cancel).toHaveBeenCalledWith("session-token", INTENT));
    expect(screen.getByRole("button", { name: "Save this meeting to CoralSwarm" })).toBeEnabled();
  });

  it("if turning off fails on Outlook's side, the choice is kept and the failure is stated", async () => {
    const d = draft({ data: { [NONCE_PROPERTY]: NONCE, [INTENT_ID_PROPERTY]: INTENT } });
    const t = setup(d);
    await screen.findByRole("button", { name: "Turn off" });
    d.props.saveAsync.mockImplementation((cb: (r: { status: string; value: unknown }) => void) => cb({ status: "failed", value: undefined }));
    fireEvent.click(screen.getByRole("button", { name: "Turn off" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Couldn’t turn this off.");
    expect(t.data[NONCE_PROPERTY]).toBe(NONCE); // local bag restored
  });
});

describe("recurring meetings", () => {
  it("a draft that already repeats says it covers the series and gives the series-level instruction", async () => {
    setup(draft({ recurrence: { recurrenceType: "weekly" } }));
    expect(await screen.findByText("Saves every meeting in this series")).toBeVisible();
    expect(screen.getByTestId("transcribe-prompt")).toHaveTextContent("Open Meeting options for this series");
    fireEvent.click(screen.getByRole("button", { name: "Save this meeting to CoralSwarm" }));
    expect(await screen.findByText("On — CoralSwarm saves the transcript of every meeting in this series after you save or send it.")).toBeVisible();
  });

  it("the server's recurring answer wins on reopen, and the next meeting is shown", async () => {
    calls.status.mockResolvedValue({ ...idle, status: "active", recurring: true, next_occurrence_start: "2099-01-05T15:00:00Z", auto_transcription: "manual", manual_reason: "recurring" });
    setup(draft({ data: { [NONCE_PROPERTY]: NONCE, [INTENT_ID_PROPERTY]: INTENT } }));
    expect(await screen.findByText("On — CoralSwarm is saving every meeting in this series.")).toBeVisible();
    expect(screen.getByText("Saves every meeting in this series")).toBeVisible();
    expect(screen.getByText(/^Next meeting:/)).toBeVisible();
    expect(screen.getByTestId("transcribe-prompt")).toHaveTextContent("Otherwise, in each meeting open More");
  });
});

describe("recording prompt vs status", () => {
  const reopened = () => draft({ data: { [NONCE_PROPERTY]: NONCE, [INTENT_ID_PROPERTY]: INTENT } });

  it("auto_transcription=enabled replaces the amber prompt with the reassuring hint", async () => {
    calls.status.mockResolvedValue({ ...idle, status: "active", auto_transcription: "enabled" });
    setup(reopened());
    expect(await screen.findByText("Teams will record and transcribe it when it starts.")).toBeVisible();
    expect(screen.queryByTestId("transcribe-prompt")).not.toBeInTheDocument();
  });

  it("manual shows the amber prompt, with the reason when there is one", async () => {
    calls.status.mockResolvedValue({ ...idle, status: "active", auto_transcription: "manual", manual_reason: "not_organizer" });
    setup(reopened());
    const prompt = await screen.findByTestId("transcribe-prompt");
    await vi.waitFor(() => expect(prompt).toHaveTextContent("Only the organizer can turn this on in Teams."));
    expect(prompt).toHaveTextContent("Turn on transcription");
  });

  it("an unknown manual_reason from the server never resolves to a prototype property", async () => {
    calls.status.mockResolvedValue({ ...idle, status: "active", auto_transcription: "manual", manual_reason: "constructor" });
    setup(reopened());
    expect(await screen.findByTestId("transcribe-prompt")).toHaveTextContent("Turn on Record and transcribe automatically in Teams Meeting options.");
  });

  it("never names a fixed time for the meeting (it may be moved)", async () => {
    setup(reopened());
    const text = (await screen.findByText(/^On — /)).closest("body")?.textContent ?? "";
    expect(text).not.toMatch(/\b\d{1,2}(:\d\d)?\s?(am|pm)\b/i);
  });
});

describe("status polling", () => {
  it("polls while waiting for the save, then goes quiet after a terminal state", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    setup(draft({ data: { [NONCE_PROPERTY]: NONCE, [INTENT_ID_PROPERTY]: INTENT } }));
    await screen.findByText(/^On — /);
    await vi.waitFor(() => expect(calls.status).toHaveBeenCalledTimes(1));
    calls.status.mockResolvedValue({ ...idle, status: "active", auto_transcription: "enabled" });
    await act(async () => { await vi.advanceTimersByTimeAsync(COMPOSE_POLL_MS + 100); });
    expect(await screen.findByText("On — CoralSwarm is saving this meeting’s transcript.")).toBeVisible();
    expect(calls.status).toHaveBeenCalledTimes(2);
    calls.status.mockResolvedValue({ ...idle, status: "stopped" });
    await act(async () => { await vi.advanceTimersByTimeAsync(20_000); });
    expect(await screen.findByText("Stopped saving. Transcripts already saved stay in your workspace.")).toBeVisible();
    const seen = calls.status.mock.calls.length;
    await act(async () => { await vi.advanceTimersByTimeAsync(60_000); });
    expect(calls.status.mock.calls.length).toBe(seen);
  });

  it("an expired intent says to turn it on again, an errored one to reset it", async () => {
    calls.status.mockResolvedValue({ ...idle, status: "expired" });
    setup(draft({ data: { [NONCE_PROPERTY]: NONCE, [INTENT_ID_PROPERTY]: INTENT } }));
    expect(await screen.findByText("This expired before CoralSwarm could find the meeting’s Teams link. Turn it on again.")).toBeVisible();
    expect(screen.queryByTestId("transcribe-prompt")).not.toBeInTheDocument();
  });

  it("a failed status read keeps the On state and says the status could not be checked", async () => {
    calls.status.mockRejectedValue(new Error("offline"));
    setup(draft({ data: { [NONCE_PROPERTY]: NONCE, [INTENT_ID_PROPERTY]: INTENT } }));
    expect(await screen.findByText("Couldn’t check this meeting’s status. Your choice is kept.")).toBeVisible();
    expect(screen.getByText(/^On — CoralSwarm saves the transcript after you save/)).toBeVisible();
  });
});

describe("gates", () => {
  it("a personal workspace is sent to choose an organization workspace; nothing is called", () => {
    const t = setup(draft(), personal);
    expect(screen.getByText("Choose an organization workspace to save meetings")).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "Change workspace" }));
    expect(t.openWorkspacePicker).toHaveBeenCalled();
    expect(calls.create).not.toHaveBeenCalled();
  });

  it("a client without CustomProperties gets a clear message and the save-then-reopen fallback; no intent is created", () => {
    setup(draft({ customProps: false }));
    expect(screen.getByText("This version of Outlook can’t turn CoralSwarm on for an unsaved meeting.")).toBeVisible();
    expect(screen.getByText("Save the meeting, then reopen CoralSwarm.")).toBeVisible();
    expect(screen.queryByRole("button", { name: "Save this meeting to CoralSwarm" })).not.toBeInTheDocument();
    expect(calls.create).not.toHaveBeenCalled();
  });
});

describe("server contract edges", () => {
  const reopened = () => draft({ data: { [NONCE_PROPERTY]: NONCE, [INTENT_ID_PROPERTY]: INTENT } });

  it("a marker with only a nonce is resolved through by-nonce", async () => {
    calls.byNonce.mockResolvedValue({ ...idle, intent_id: INTENT });
    calls.status.mockResolvedValue({ ...idle, status: "active", auto_transcription: "enabled" });
    setup(draft({ data: { [NONCE_PROPERTY]: NONCE } }));
    expect(await screen.findByText("On — CoralSwarm is saving this meeting’s transcript.")).toBeVisible();
    expect(calls.byNonce).toHaveBeenCalledWith("session-token", NONCE);
    expect(calls.status).toHaveBeenCalledWith("session-token", INTENT, expect.anything());
  });

  it("an intent the server does not know (404) is shown as needing a reset, with Turn off available", async () => {
    calls.status.mockRejectedValue(new ApiError("nope", 404, null));
    setup(reopened());
    expect(await screen.findByText("CoralSwarm couldn’t follow this meeting. Turn it off and on again.")).toBeVisible();
    expect(screen.getByRole("button", { name: "Turn off" })).toBeEnabled();
  });


  it("stopped with a reason explains it", async () => {
    calls.status.mockResolvedValue({ ...idle, status: "stopped", error_code: "event_cancelled" });
    setup(reopened());
    expect(await screen.findByText("This meeting was cancelled, so saving has stopped.")).toBeVisible();
  });

  it("429 caps are explained and leave the Save button usable", async () => {
    calls.create.mockRejectedValue(new ApiError("x", 429, "too_many_pending"));
    setup();
    fireEvent.click(await screen.findByRole("button", { name: "Save this meeting to CoralSwarm" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("too many unsaved meetings");
    expect(screen.getByRole("button", { name: "Save this meeting to CoralSwarm" })).toBeEnabled();
  });

  it("connect_required offers Connect Microsoft (only to login.microsoftonline.com) and writes no marker", async () => {
    calls.create.mockRejectedValue(new ApiError("x", 403, "connect_required"));
    calls.connect.mockResolvedValue({ authorization_url: "https://login.microsoftonline.com/common/oauth2/v2.0/authorize?x=1" });
    const t = setup();
    fireEvent.click(await screen.findByRole("button", { name: "Save this meeting to CoralSwarm" }));
    fireEvent.click(await screen.findByRole("button", { name: "Connect Microsoft" }));
    await vi.waitFor(() => expect(t.openAuthentication).toHaveBeenCalledWith(expect.stringMatching(/^https:\/\/login\.microsoftonline\.com\//)));
    expect(calls.connect).toHaveBeenCalledWith("session-token", "/outlook/meeting/connected");
    expect(t.data[NONCE_PROPERTY]).toBeUndefined();
  });
});

describe("review fixes", () => {
  const nonceOnly = () => draft({ data: { [NONCE_PROPERTY]: NONCE } });

  it("a transient by-nonce failure is shown and retried, never treated as local-only", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    calls.byNonce.mockRejectedValueOnce(new Error("offline")).mockResolvedValue({ ...idle, intent_id: INTENT });
    setup(nonceOnly());
    expect(await screen.findByText("Couldn’t check this meeting’s status. Your choice is kept.")).toBeVisible();
    await act(async () => { await vi.advanceTimersByTimeAsync(COMPOSE_POLL_MS + 100); });
    await vi.waitFor(() => expect(calls.status).toHaveBeenCalledWith("session-token", INTENT, expect.anything()));
    expect(calls.byNonce).toHaveBeenCalledTimes(2);
  });

  it("Turn off on a nonce-only marker resolves the intent by nonce, then cancels it on the server", async () => {
    calls.byNonce.mockRejectedValueOnce(new Error("offline")).mockResolvedValue({ ...idle, intent_id: INTENT });
    const t = setup(nonceOnly());
    fireEvent.click(await screen.findByRole("button", { name: "Turn off" }));
    expect(await screen.findByRole("button", { name: "Save this meeting to CoralSwarm" })).toBeVisible();
    expect(calls.cancel).toHaveBeenCalledWith("session-token", INTENT);
    expect(t.data[NONCE_PROPERTY]).toBeUndefined();
  });

  it("if the nonce cannot be resolved, Turn off fails and keeps the marker rather than removing it locally", async () => {
    calls.byNonce.mockRejectedValue(new Error("offline"));
    const t = setup(nonceOnly());
    fireEvent.click(await screen.findByRole("button", { name: "Turn off" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Couldn’t turn this off.");
    expect(calls.cancel).not.toHaveBeenCalled();
    expect(t.data[NONCE_PROPERTY]).toBe(NONCE);
  });

  it("a 404 from by-nonce means the server has none: Turn off just clears the marker", async () => {
    calls.byNonce.mockRejectedValue(new ApiError("nope", 404, null));
    const t = setup(nonceOnly());
    fireEvent.click(await screen.findByRole("button", { name: "Turn off" }));
    expect(await screen.findByRole("button", { name: "Save this meeting to CoralSwarm" })).toBeVisible();
    expect(calls.cancel).not.toHaveBeenCalled();
    expect(t.data[NONCE_PROPERTY]).toBeUndefined();
  });

  it("a marker write that times out does NOT cancel the server intent (the save may still land)", async () => {
    const d = draft();
    d.props.saveAsync.mockImplementation(() => {}); // Outlook never answers
    vi.useFakeTimers({ shouldAdvanceTime: true });
    setup(d);
    fireEvent.click(await screen.findByRole("button", { name: "Save this meeting to CoralSwarm" }));
    await act(async () => { await vi.advanceTimersByTimeAsync(31_000); });
    expect(await screen.findByRole("alert")).toHaveTextContent("Outlook is slow to confirm this.");
    expect(calls.cancel).not.toHaveBeenCalled();
    expect(screen.getByRole("button", { name: "Turn off" })).toBeVisible();
  });
});

describe("waiting states", () => {
  const reopened = (extra: Record<string, string> = {}) => draft({ data: { [NONCE_PROPERTY]: NONCE, [INTENT_ID_PROPERTY]: INTENT, ...extra } });
  const NEW_NONCE = "33333333-3333-4333-a333-333333333333";
  const NEW_INTENT = "44444444-4444-4444-a444-444444444444";

  it("pending without an error code keeps the plain 'saves after you save or send' line", async () => {
    setup(reopened());
    expect(await screen.findByText("On — CoralSwarm saves the transcript after you save or send this meeting.")).toBeVisible();
    expect(screen.queryByText("Waiting for a Teams link")).not.toBeInTheDocument();
  });

  it("teams_link_missing is an amber waiting state that says what to do and until when (stored expiry, local time)", async () => {
    calls.status.mockResolvedValue({ ...idle, error_code: "teams_link_missing" });
    setup(reopened({ [EXPIRES_PROPERTY]: "2099-01-01T15:42:00Z" }));
    expect(await screen.findByText("Waiting for a Teams link")).toBeVisible();
    const detail = screen.getByText(/^Saved, but this meeting doesn’t have a Teams link yet\. Add an attendee or turn on Teams meeting, then save again\./);
    expect(detail).toHaveTextContent(/CoralSwarm keeps waiting until .+\d.*\.$/);
    expect(detail).toHaveTextContent(new Intl.DateTimeFormat(undefined, { hour: "numeric", minute: "2-digit" }).format(new Date("2099-01-01T15:42:00Z")));
    expect(screen.queryByText(/^On — /)).not.toBeInTheDocument();
    expect(document.querySelector('img[src^="/addin/icons/subject--amber"]')).not.toBeNull();
    expect(screen.getByRole("button", { name: "Turn off" })).toBeEnabled();
  });

  it("an expires_at on the status wins over the stored one", async () => {
    calls.status.mockResolvedValue({ ...idle, error_code: "teams_link_missing", expires_at: "2098-06-01T08:05:00Z" });
    setup(reopened({ [EXPIRES_PROPERTY]: "2099-01-01T15:42:00Z" }));
    const detail = await screen.findByText(/keeps waiting until/);
    expect(detail).toHaveTextContent(new Intl.DateTimeFormat(undefined, { weekday: "short", month: "short", day: "numeric" }).format(new Date("2098-06-01T08:05:00Z")));
  });

  it("with no known expiry the waiting message still renders, without a time", async () => {
    calls.status.mockResolvedValue({ ...idle, error_code: "teams_link_missing" });
    setup(reopened());
    const detail = await screen.findByText(/^Saved, but this meeting doesn’t have a Teams link yet/);
    expect(detail).not.toHaveTextContent("keeps waiting");
  });

  it("an expiry that has already passed is not offered as a deadline", async () => {
    calls.status.mockResolvedValue({ ...idle, error_code: "teams_link_missing" });
    setup(reopened({ [EXPIRES_PROPERTY]: "2001-01-01T00:00:00Z" }));
    expect(await screen.findByText(/^Saved, but this meeting doesn’t have a Teams link yet/)).not.toHaveTextContent("keeps waiting");
  });

  it("turning it on stores the server's expires_at next to the marker so a reopened draft can show it", async () => {
    const t = setup();
    fireEvent.click(await screen.findByRole("button", { name: "Save this meeting to CoralSwarm" }));
    await screen.findByText(/^On — /);
    expect(t.data[EXPIRES_PROPERTY]).toBe("2099-01-01T00:00:00Z");
  });

  it("other pending codes map to their message; an unknown one falls back to the plain waiting line", async () => {
    calls.status.mockResolvedValue({ ...idle, error_code: "already_ended" });
    const first = setup(reopened());
    expect(await screen.findByText("This meeting has already ended.")).toBeVisible();
    first.unmount();
    calls.status.mockResolvedValue({ ...idle, error_code: "constructor" });
    setup(reopened());
    expect(await screen.findByText(/^On — CoralSwarm saves the transcript after you save/)).toBeVisible();
  });

  it("expired after a missing Teams link says so and offers 'Turn on again'", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    calls.status.mockResolvedValue({ ...idle, error_code: "teams_link_missing" });
    setup(reopened());
    await screen.findByText("Waiting for a Teams link");
    calls.status.mockResolvedValue({ ...idle, status: "expired" });
    await act(async () => { await vi.advanceTimersByTimeAsync(COMPOSE_POLL_MS + 100); });
    expect(await screen.findByText("This expired before the meeting got a Teams link. Turn it on again.")).toBeVisible();
    expect(screen.getByRole("button", { name: "Turn on again" })).toBeEnabled();
  });

  it("expired with no link history keeps the plain expiry copy, still with the action", async () => {
    calls.status.mockResolvedValue({ ...idle, status: "expired" });
    setup(reopened());
    expect(await screen.findByText("This expired before CoralSwarm could find the meeting’s Teams link. Turn it on again.")).toBeVisible();
    expect(screen.getByRole("button", { name: "Turn on again" })).toBeVisible();
  });

  it("'Turn on again' creates a NEW intent, replaces the marker on the draft, and goes back to waiting", async () => {
    calls.status.mockResolvedValue({ ...idle, status: "expired" });
    const t = setup(reopened({ [EXPIRES_PROPERTY]: "2001-01-01T00:00:00Z" }));
    calls.create.mockResolvedValue({ intent_id: NEW_INTENT, nonce: NEW_NONCE, expires_at: "2099-02-02T00:00:00Z" });
    fireEvent.click(await screen.findByRole("button", { name: "Turn on again" }));
    calls.status.mockResolvedValue(idle);
    expect(await screen.findByText(/^On — CoralSwarm saves the transcript after you save/)).toBeVisible();
    expect(calls.create).toHaveBeenCalledTimes(1);
    expect(t.data[NONCE_PROPERTY]).toBe(NEW_NONCE); expect(t.data[INTENT_ID_PROPERTY]).toBe(NEW_INTENT); expect(t.data[EXPIRES_PROPERTY]).toBe("2099-02-02T00:00:00Z");
    expect(t.itemSave).not.toHaveBeenCalled();
    await vi.waitFor(() => expect(calls.status).toHaveBeenCalledWith("session-token", NEW_INTENT, expect.anything()));
  });

  it("if turning on again fails, the expired state stays and the failure is stated", async () => {
    calls.status.mockResolvedValue({ ...idle, status: "expired" });
    const t = setup(reopened());
    calls.create.mockRejectedValue(new ApiError("x", 429, "too_many_pending"));
    fireEvent.click(await screen.findByRole("button", { name: "Turn on again" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("You have too many unsaved meetings waiting.");
    expect(screen.getByRole("button", { name: "Turn on again" })).toBeEnabled();
    expect(t.data[NONCE_PROPERTY]).toBe(NONCE);
  });

  it("if Outlook refuses the new marker, the new intent is cancelled and the old marker survives", async () => {
    calls.status.mockResolvedValue({ ...idle, status: "expired" });
    const d = reopened();
    const t = setup(d);
    calls.create.mockResolvedValue({ intent_id: NEW_INTENT, nonce: NEW_NONCE, expires_at: "2099-02-02T00:00:00Z" });
    await screen.findByRole("button", { name: "Turn on again" });
    d.props.saveAsync.mockImplementation((cb: (r: { status: string; value: unknown }) => void) => cb({ status: "failed", value: undefined }));
    fireEvent.click(screen.getByRole("button", { name: "Turn on again" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Couldn’t turn this on. Try again.");
    await vi.waitFor(() => expect(calls.cancel).toHaveBeenCalledWith("session-token", NEW_INTENT));
    expect(t.data[NONCE_PROPERTY]).toBe(NONCE);
  });

  it("turning on again before Microsoft is connected keeps the marker and offers Connect Microsoft and Check connection", async () => {
    calls.status.mockResolvedValue({ ...idle, status: "expired" });
    const t = setup(reopened());
    calls.create.mockRejectedValue(new ApiError("x", 403, "connect_required"));
    fireEvent.click(await screen.findByRole("button", { name: "Turn on again" }));
    expect(await screen.findByText("Connect your Microsoft account, then come back. Connecting doesn’t save anything.")).toBeVisible();
    expect(screen.getByRole("button", { name: "Connect Microsoft" })).toBeEnabled();
    expect(screen.queryByRole("button", { name: "Turn on again" })).not.toBeInTheDocument();
    expect(t.data[NONCE_PROPERTY]).toBe(NONCE);
    // Check connection retries the same turn-on-again and succeeds once connected.
    calls.create.mockResolvedValue({ intent_id: NEW_INTENT, nonce: NEW_NONCE, expires_at: "2099-02-02T00:00:00Z" });
    calls.status.mockResolvedValue(idle);
    fireEvent.click(screen.getByRole("button", { name: "Check connection" }));
    expect(await screen.findByText(/^On — CoralSwarm saves the transcript after you save/)).toBeVisible();
    expect(t.data[NONCE_PROPERTY]).toBe(NEW_NONCE);
  });

  it("Connect Microsoft from the expired state opens the Microsoft sign-in", async () => {
    calls.status.mockResolvedValue({ ...idle, status: "expired" });
    const t = setup(reopened());
    calls.create.mockRejectedValue(new ApiError("x", 403, "connect_required"));
    calls.connect.mockResolvedValue({ authorization_url: "https://login.microsoftonline.com/common/oauth2/authorize?x=1" });
    fireEvent.click(await screen.findByRole("button", { name: "Turn on again" }));
    fireEvent.click(await screen.findByRole("button", { name: "Connect Microsoft" }));
    await vi.waitFor(() => expect(t.openAuthentication).toHaveBeenCalledWith("https://login.microsoftonline.com/common/oauth2/authorize?x=1"));
  });

  it("on reopen (link history unknown) an expiry never claims the meeting was not saved", async () => {
    calls.status.mockResolvedValue({ ...idle, status: "expired" });
    setup(reopened());
    const line = await screen.findByText(/^This expired before /);
    expect(line).not.toHaveTextContent("before the meeting was saved");
    expect(line).toHaveTextContent("could find the meeting’s Teams link");
  });

  it.each([
    ["stopped", "capture_stopped", "Saving was stopped from the Teams meeting pane."],
    ["stopped", "superseded", "This meeting was turned on again, so this earlier choice stopped."],
    ["stopped", "completed", "This meeting has finished."],
    ["error", "ambiguous_nonce", "More than one event carries this choice"],
    ["error", "unsupported_event", "CoralSwarm can’t follow this kind of event."],
    ["error", "unsupported_join_url", "This event’s Teams link isn’t one CoralSwarm can follow."],
    ["error", "not_organizer", "Only the organizer can save this meeting’s transcript."],
    ["error", "already_ended", "This meeting has already ended."],
    ["error", "never_heard_of_it", "CoralSwarm couldn’t follow this meeting."],
  ])("%s + %s is explained", async (state, code, text) => {
    calls.status.mockResolvedValue({ ...idle, status: state, error_code: code });
    setup(reopened());
    expect(await screen.findByText(new RegExp(`^${text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}`))).toBeVisible();
  });
});

describe("reopened saved organizer appointment (prod e2e regression)", () => {
  const saved = () => draft({ data: { [NONCE_PROPERTY]: NONCE, [INTENT_ID_PROPERTY]: INTENT, [EXPIRES_PROPERTY]: "2099-01-01T15:42:00Z" } });
  const visibility = (value: "visible" | "hidden") => Object.defineProperty(document, "visibilityState", { value, configurable: true });
  afterEach(() => visibility("visible"));

  it("marker with the intent id + pending_save/teams_link_missing from the API renders the amber state", async () => {
    calls.status.mockResolvedValue({ status: "pending_save", error_code: "teams_link_missing", recurring: false, next_occurrence_start: null, auto_transcription: null, manual_reason: null });
    setup(saved());
    expect(await screen.findByText("Waiting for a Teams link")).toBeVisible();
    expect(screen.getByText(/CoralSwarm keeps waiting until/)).toBeVisible();
    expect(calls.byNonce).not.toHaveBeenCalled();
    expect(calls.status).toHaveBeenCalledWith("session-token", INTENT, expect.anything());
  });

  it("asks for the first status even when the host reports the document as hidden", async () => {
    visibility("hidden");
    calls.status.mockResolvedValue({ ...idle, error_code: "teams_link_missing" });
    setup(saved());
    expect(await screen.findByText("Waiting for a Teams link")).toBeVisible();
  });

  it("a status request stuck in the token exchange shows the status-failed text within the timeout, then recovers", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    let hang = true;
    setup(saved(), org, (fn) => (hang ? new Promise(() => {}) : fn("session-token")));
    await screen.findByText(/^On — /);
    expect(screen.queryByText("Couldn’t check this meeting’s status. Your choice is kept.")).not.toBeInTheDocument();
    await act(async () => { await vi.advanceTimersByTimeAsync(COMPOSE_STATUS_TIMEOUT_MS + 100); });
    expect(await screen.findByText("Couldn’t check this meeting’s status. Your choice is kept.")).toBeVisible();
    // The wedged request no longer blocks polling: the next poll goes through and the amber state appears.
    hang = false;
    calls.status.mockResolvedValue({ ...idle, error_code: "teams_link_missing" });
    await act(async () => { await vi.advanceTimersByTimeAsync(COMPOSE_POLL_MS + 100); });
    expect(await screen.findByText("Waiting for a Teams link")).toBeVisible();
    expect(screen.queryByText("Couldn’t check this meeting’s status. Your choice is kept.")).not.toBeInTheDocument();
  });

  it("a hung by-nonce lookup is retried: the next attempt resolves the marker and starts polling status", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    let call = 0;
    calls.byNonce.mockResolvedValue({ ...idle, error_code: "teams_link_missing", intent_id: INTENT });
    calls.status.mockResolvedValue({ ...idle, error_code: "teams_link_missing" });
    setup(draft({ data: { [NONCE_PROPERTY]: NONCE } }), org, (fn) => (++call === 1 ? new Promise(() => {}) : fn("session-token")));
    await screen.findByText(/^On — /);
    await act(async () => { await vi.advanceTimersByTimeAsync(COMPOSE_STATUS_TIMEOUT_MS + 100); });
    expect(await screen.findByText("Couldn’t check this meeting’s status. Your choice is kept.")).toBeVisible();
    await act(async () => { await vi.advanceTimersByTimeAsync(COMPOSE_POLL_MS + 100); });
    expect(calls.byNonce).toHaveBeenCalledTimes(1);
    await vi.waitFor(() => expect(calls.status).toHaveBeenCalledWith("session-token", INTENT, expect.anything()));
    expect(await screen.findByText("Waiting for a Teams link")).toBeVisible();
    expect(screen.queryByText("Couldn’t check this meeting’s status. Your choice is kept.")).not.toBeInTheDocument();
  });

  it("a nonce-only marker whose lookup hangs also reports it", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    setup(draft({ data: { [NONCE_PROPERTY]: NONCE } }), org, () => new Promise(() => {}));
    await screen.findByText(/^On — /);
    await act(async () => { await vi.advanceTimersByTimeAsync(COMPOSE_STATUS_TIMEOUT_MS + 100); });
    expect(await screen.findByText("Couldn’t check this meeting’s status. Your choice is kept.")).toBeVisible();
  });
});
