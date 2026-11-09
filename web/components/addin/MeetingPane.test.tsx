// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MeetingPane, POLL_MS } from "./MeetingPane";
import { Surface } from "./kit";
import type { ConnectedContext } from "./AddinApp";
import { ReconnectRequiredError, type AddinClient, type Workspace } from "@/lib/addin/microsoftSso";
import { ApiError } from "@/lib/api";

const calls = vi.hoisted(() => ({ status: vi.fn(), capture: vi.fn(), stop: vi.fn(), connect: vi.fn() }));
vi.mock("@/lib/api", async () => ({
  ...(await vi.importActual<typeof import("@/lib/api")>("@/lib/api")),
  api: { msTeams: { meetingStatus: calls.status, captureMeeting: calls.capture, stopMeetingSelection: calls.stop, connectMeeting: calls.connect } },
}));

const org: Workspace = { account_id: "acct-o", name: "Acme ISD", kind: "org" };
const personal: Workspace = { account_id: "acct-p", name: "Personal", kind: "personal" };
const selector = { teams_meeting_id: "sdk-meeting", chat_id: "sdk-chat" };
const inactive = { connection_required: false, availability_window_open: true, meeting_id: "verified", title: "Budget review", occurrence_start: "2026-10-01T10:00:00Z", occurrence_end: "2026-10-01T11:00:00Z", capture: null };
const cap = (over: Record<string, unknown> = {}) => ({ capture_id: "cap-1", status: "active" as const, last_checked_at: null, last_retrieved_at: null, retrieved_transcript_count: 0, last_error_code: null, ...over });
const active = { ...inactive, capture: cap() };

function setup(workspace: Workspace = org, extra: Partial<ConnectedContext> = {}, host: "teams" | "outlook" = "teams") {
  const toast = vi.fn();
  const reconnectRequired = vi.fn();
  const openWorkspacePicker = vi.fn();
  const withToken = vi.fn(async (fn: (token: string) => Promise<unknown>) => fn("session-token"));
  const context = {
    session: { token: "session-token", expiresAt: Date.now() + 3600_000, account: { email: "ada@acme.test" }, workspace, autoLinked: false },
    client: { withToken } as unknown as AddinClient, toast, reconnectRequired, openWorkspacePicker, ...extra,
  } as ConnectedContext;
  const openAuthentication = vi.fn(async () => {});
  const ui = (ctx = context) => (
    <Surface label="test"><MeetingPane context={ctx} selector={selector} host={host} openAuthentication={openAuthentication} connectionReturnPath="/teams/meeting/connected" /></Surface>
  );
  const view = render(ui());
  return { ...view, toast, reconnectRequired, openWorkspacePicker, withToken, openAuthentication, ui, context };
}

beforeEach(() => { vi.clearAllMocks(); calls.status.mockResolvedValue(inactive); });
afterEach(() => { cleanup(); vi.useRealTimers(); });

describe("workspace rule", () => {
  it("a personal workspace reaches the same explicit private consent and saves nothing on open", async () => {
    calls.capture.mockResolvedValue({ meeting_id: "verified", capture_id: "new", status: "waiting_for_transcript" });
    const t = setup(personal, {}, "outlook");
    const save = await screen.findByRole("button", { name: "Save meeting to CoralSwarm" });
    expect(screen.getByText(/Only you can see this transcript, in your personal workspace/)).toBeVisible();
    expect(screen.getByText(/work or school accounts only/)).toBeVisible();
    expect(screen.queryByText("Choose an organization workspace to save meetings")).not.toBeInTheDocument();
    expect(calls.capture).not.toHaveBeenCalled();
    fireEvent.click(save);
    await vi.waitFor(() => expect(calls.capture).toHaveBeenCalledWith("session-token", { meeting_id: "verified" }));
    await vi.waitFor(() => expect(t.toast).toHaveBeenCalled());
  });

  it("an organization workspace is unchanged and shows no personal wording", async () => {
    setup(org);
    expect(await screen.findByRole("button", { name: "Save this meeting" })).toBeVisible();
    expect(screen.queryByText(/Only you can see this transcript/)).not.toBeInTheDocument();
  });

  it("states the provider limit when Microsoft is a consumer account, on read and on save", async () => {
    calls.status.mockRejectedValueOnce(new ApiError("refused", 403, "consumer_identity_unsupported"));
    setup(personal);
    expect(await screen.findByRole("alert")).toHaveTextContent(/only shares meeting transcripts for work or school accounts/);
    expect(screen.queryByRole("button", { name: "Connect Microsoft" })).not.toBeInTheDocument();
    cleanup(); calls.status.mockResolvedValue(inactive);
    calls.capture.mockRejectedValue(new ApiError("refused", 403, "consumer_identity_unsupported"));
    setup(personal);
    fireEvent.click(await screen.findByRole("button", { name: "Save this meeting" }));
    expect(await screen.findByRole("alert")).toHaveTextContent(/only shares meeting transcripts for work or school accounts/);
    await vi.waitFor(() => expect(calls.status).toHaveBeenCalledTimes(3));
    expect(screen.getByRole("alert")).toHaveTextContent(/work or school accounts/);
  });

  it("shows an unrecognized refusal generically, never its code or message", async () => {
    calls.capture.mockRejectedValue(new ApiError("organization_required internal", 403, "organization_required"));
    setup(personal);
    fireEvent.click(await screen.findByRole("button", { name: "Save this meeting" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Couldn’t check this meeting. Try again — nothing has changed.");
    expect(screen.queryByText(/organization_required|internal/)).not.toBeInTheDocument();
  });

  it("a switch from organization to personal discards the old read and aborts it", async () => {
    let late!: (value: typeof inactive) => void;
    calls.status.mockImplementationOnce(() => new Promise((r) => { late = r; })).mockResolvedValue({ ...inactive, title: "Personal meeting" });
    const t = setup(org);
    await vi.waitFor(() => expect(calls.status).toHaveBeenCalledTimes(1));
    const signal = calls.status.mock.calls[0][2] as AbortSignal;
    t.rerender(t.ui({ ...t.context, session: { ...t.context.session, workspace: personal } }));
    expect(await screen.findByText("Personal meeting")).toBeVisible();
    await act(async () => { late({ ...inactive, title: "Org meeting" }); });
    expect(signal.aborted).toBe(true);
    expect(screen.queryByText("Org meeting")).not.toBeInTheDocument();
  });

  it("never sends a save if the workspace changes while the token is still being minted", async () => {
    // Index 0 is the old controller's read; index 1 is its save, held while the token is "minted".
    const pending: Array<(token: string) => void> = [];
    const withToken = vi.fn((fn: (token: string) => Promise<unknown>) => {
      if (pending.length === 0 && withToken.mock.calls.length === 1) { pending.push(() => {}); return fn("org-token"); }
      return new Promise((resolve, reject) => { pending.push((token) => void Promise.resolve().then(() => fn(token)).then(resolve, reject)); });
    });
    const t = setup(org, { client: { withToken } as unknown as AddinClient });
    fireEvent.click(await screen.findByRole("button", { name: "Save this meeting" }));
    t.rerender(t.ui({ ...t.context, session: { ...t.context.session, workspace: personal } }));
    await act(async () => { pending[1]("stale-org-token"); });
    expect(calls.capture).not.toHaveBeenCalled();
    expect(t.toast).not.toHaveBeenCalled();
  });
});

describe("save / stop", () => {
  it("verifies read-only first, shows title + workspace, and saves only the backend-verified meeting on tap", async () => {
    calls.capture.mockResolvedValue({ meeting_id: "verified", capture_id: "new", status: "waiting_for_transcript" });
    const t = setup();
    expect(await screen.findByText("Budget review")).toBeVisible();
    expect(screen.getByText("Acme ISD")).toBeVisible();
    expect(screen.getByText("Not saved yet")).toBeVisible();
    expect(calls.status.mock.calls[0][0]).toBe("session-token");
    expect(calls.status.mock.calls[0][1]).toEqual(selector);
    fireEvent.click(screen.getByRole("button", { name: "Save this meeting" }));
    await vi.waitFor(() => expect(calls.capture).toHaveBeenCalledWith("session-token", { meeting_id: "verified" }));
    await vi.waitFor(() => expect(t.toast).toHaveBeenCalledWith("Saving this meeting — its transcript follows on its own."));
  });

  it("the Outlook appointment button is labelled 'Save meeting to CoralSwarm'", async () => {
    setup(org, {}, "outlook");
    expect(await screen.findByRole("button", { name: "Save meeting to CoralSwarm" })).toBeVisible();
  });

  it("active save shows 'In your workspace · waiting for transcript' and Stop, never a second consent", async () => {
    calls.status.mockResolvedValue(active); calls.stop.mockResolvedValue({ status: "revoked" });
    const t = setup();
    expect(await screen.findByText("In your workspace · waiting for transcript")).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "Stop saving" }));
    await vi.waitFor(() => expect(calls.stop).toHaveBeenCalledWith("session-token", "cap-1"));
    expect(calls.capture).not.toHaveBeenCalled();
    await vi.waitFor(() => expect(t.toast).toHaveBeenCalledWith("Stopped saving. Transcripts already saved stay in your workspace."));
  });

  it("a retrieved transcript reads 'In your workspace · transcript saved'", async () => {
    calls.status.mockResolvedValue({ ...inactive, capture: cap({ last_retrieved_at: "2026-10-01T11:01:00Z", last_checked_at: "2026-10-01T11:01:00Z", retrieved_transcript_count: 2 }) });
    setup();
    expect(await screen.findByText("In your workspace · transcript saved")).toBeVisible();
    expect(screen.getByText("2 transcripts retrieved from Microsoft at the last check.")).toBeVisible();
  });

  it("stopped capture reads 'Stopped saving' and offers Save again", async () => {
    calls.status.mockResolvedValue({ ...inactive, capture: cap({ status: "stopped" }) });
    setup();
    expect(await screen.findByText("Stopped saving")).toBeVisible();
    expect(screen.getByRole("button", { name: "Save this meeting" })).toBeEnabled();
  });
});

describe("states in Gmail's voice", () => {
  it("window ended: no Save", async () => {
    calls.status.mockResolvedValue({ ...inactive, availability_window_open: false });
    setup();
    expect(await screen.findByText("Saving window ended")).toBeVisible();
    expect(screen.getByRole("button", { name: "Save this meeting" })).toBeDisabled();
  });

  it("Microsoft connection needed: Connect Microsoft opens only a login.microsoftonline.com URL", async () => {
    calls.status.mockResolvedValue({ ...active, connection_required: true });
    calls.connect.mockResolvedValue({ authorization_url: "https://login.microsoftonline.com/common/oauth2/authorize?x=1" });
    const t = setup();
    expect(await screen.findByText("Microsoft connection needed")).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "Connect Microsoft" }));
    await vi.waitFor(() => expect(t.openAuthentication).toHaveBeenCalledWith("https://login.microsoftonline.com/common/oauth2/authorize?x=1"));
    expect(calls.connect).toHaveBeenCalledWith("session-token", "/teams/meeting/connected");
  });

  it("refuses a foreign sign-in destination", async () => {
    calls.status.mockResolvedValue({ ...inactive, connection_required: true });
    calls.connect.mockResolvedValue({ authorization_url: "https://evil.test/login" });
    const t = setup();
    fireEvent.click(await screen.findByRole("button", { name: "Connect Microsoft" }));
    expect(await screen.findByText("Microsoft sign-in could not open. Try again.")).toBeVisible();
    expect(t.openAuthentication).not.toHaveBeenCalled();
  });

  it("maps backend error codes to Gmail-voice copy", async () => {
    calls.status.mockRejectedValue(new ApiError("x", 409, "occurrence_unverified"));
    setup();
    expect(await screen.findByText("Microsoft couldn’t confirm this meeting yet. Try again in a minute.")).toBeVisible();
    expect(screen.getByRole("button", { name: "Check again" })).toBeVisible();
  });

  it("uses no 'capture' vocabulary on screen", async () => {
    calls.status.mockResolvedValue(active);
    setup();
    await screen.findByText("In your workspace · waiting for transcript");
    expect(document.body.textContent).not.toMatch(/capture|Grow|Change ocean/i);
  });
});

describe("session handling", () => {
  it("a lost link goes back to the start card instead of rendering an error", async () => {
    const t = setup(org, { client: { withToken: vi.fn(async () => { throw new ReconnectRequiredError(); }) } as unknown as AddinClient });
    await vi.waitFor(() => expect(t.reconnectRequired).toHaveBeenCalled());
  });

  it("polls an owned capture id every 15 s while active", async () => {
    vi.useFakeTimers();
    calls.status.mockResolvedValueOnce(active).mockResolvedValue({ ...active, title: null, capture: cap({ last_retrieved_at: "2026-10-01T11:01:00Z", retrieved_transcript_count: 1 }) });
    setup(); await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    expect(screen.getByText("In your workspace · waiting for transcript")).toBeVisible();
    await act(async () => { await vi.advanceTimersByTimeAsync(POLL_MS); });
    expect(calls.status.mock.calls[1][1]).toEqual({ capture_id: "cap-1" });
    expect(screen.getByText("In your workspace · transcript saved")).toBeVisible();
    expect(screen.getByText("Budget review")).toBeVisible(); // title kept when the poll omits it
  });

  it("does not poll while capture is off", async () => {
    vi.useFakeTimers();
    setup(); await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    await act(async () => { await vi.advanceTimersByTimeAsync(60_000); });
    expect(calls.status).toHaveBeenCalledTimes(1);
  });

  it("a workspace change aborts the old read and never shows the old workspace's state", async () => {
    let resolve!: (value: typeof active) => void;
    calls.status.mockImplementationOnce(() => new Promise((r) => { resolve = r; })).mockResolvedValue(inactive);
    const t = setup();
    await vi.waitFor(() => expect(calls.status).toHaveBeenCalledTimes(1));
    const signal = calls.status.mock.calls[0][2] as AbortSignal;
    const otherOrg = { ...org, account_id: "acct-o2", name: "Other ISD" };
    t.rerender(t.ui({ ...t.context, session: { ...t.context.session, workspace: otherOrg } }));
    await screen.findByRole("button", { name: "Save this meeting" });
    await act(async () => { resolve(active); });
    expect(signal.aborted).toBe(true);
    expect(screen.queryByRole("button", { name: "Stop saving" })).not.toBeInTheDocument();
    expect(screen.getByText("Other ISD")).toBeVisible();
  });
});

describe("recording prompt (Outlook saved meeting)", () => {
  it("shows the amber 'Turn on transcription' prompt before saving and while saving is on (manual)", async () => {
    setup(org, {}, "outlook");
    const prompt = await screen.findByTestId("transcribe-prompt");
    expect(prompt).toHaveTextContent("Turn on transcription");
    expect(prompt).toHaveTextContent("In the meeting, open More (…) → Record and transcribe → Start transcription.");
    expect(prompt).toHaveTextContent("CoralSwarm saves the transcript after the meeting.");
    cleanup();
    calls.status.mockResolvedValue({ ...active, auto_transcription: "manual" });
    setup(org, {}, "outlook");
    expect(await screen.findByText("In your workspace · waiting for transcript")).toBeVisible();
    expect(screen.getByTestId("transcribe-prompt")).toBeVisible();
  });

  it("auto_transcription=enabled shows the reassuring hint instead", async () => {
    calls.status.mockResolvedValue({ ...active, auto_transcription: "enabled" });
    setup(org, {}, "outlook");
    expect(await screen.findByText("Teams will record and transcribe it when it starts.")).toBeVisible();
    expect(screen.queryByTestId("transcribe-prompt")).not.toBeInTheDocument();
  });

  it("is not shown once saving has stopped or the window has closed", async () => {
    calls.status.mockResolvedValue({ ...inactive, capture: cap({ status: "stopped" }) });
    setup(org, {}, "outlook");
    expect(await screen.findByText("Stopped saving")).toBeVisible();
    expect(screen.queryByTestId("transcribe-prompt")).not.toBeInTheDocument();
  });

  it("the Teams host keeps its own pane's prompt (MeetingPane adds none)", async () => {
    setup(org, {}, "teams");
    await screen.findByText("Not saved yet");
    expect(screen.queryByTestId("transcribe-prompt")).not.toBeInTheDocument();
  });
});
