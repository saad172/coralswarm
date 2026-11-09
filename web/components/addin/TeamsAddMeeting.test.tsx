// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { ApiError } from "@/lib/api";
import { copy } from "@/lib/addin/copy";
import type { ConnectedContext } from "./AddinApp";
import { TeamsAddMeeting } from "./TeamsAddMeeting";

const calls = vi.hoisted(() => ({ status: vi.fn(), capture: vi.fn(), connect: vi.fn() }));
vi.mock("@/lib/api", async () => ({ ...(await vi.importActual<typeof import("@/lib/api")>("@/lib/api")), api: { msTeams: { meetingStatus: calls.status, captureMeeting: calls.capture, connectMeeting: calls.connect } } }));

const selector = { teams_meeting_id: "sdk-meeting", chat_id: "sdk-chat" };
const ready = { meeting_id: "verified-meeting", title: "Standup", occurrence_start: "2026-10-01T10:00:00Z", occurrence_end: "2026-10-01T11:00:00Z", availability_window_open: true, connection_required: false, recording_active: true, capture: null };
const added = { meeting_id: "verified-meeting", capture_id: "cap", status: "waiting_for_transcript" as const };

function context(kind: "personal" | "org", accountId: string, withToken?: ConnectedContext["client"]["withToken"]): ConnectedContext {
  const client = { withToken: withToken ?? (async (fn: (token: string) => Promise<unknown>) => fn(`token-${kind}-${accountId}`)) } as unknown as ConnectedContext["client"];
  return {
    session: { token: "session", expiresAt: Date.now() + 60_000, account: { email: "owner@example.test" }, workspace: { account_id: accountId, name: kind === "org" ? "Org" : "Personal", kind }, autoLinked: false },
    client, toast: vi.fn(), reconnectRequired: vi.fn(), openWorkspacePicker: vi.fn(),
  } as ConnectedContext;
}
const pane = (ctx: ConnectedContext) => <TeamsAddMeeting context={ctx} selector={selector} connectionReturnPath="/teams/meeting/connected" openAuthentication={vi.fn()} />;

beforeEach(() => { vi.clearAllMocks(); calls.status.mockResolvedValue(ready); calls.capture.mockResolvedValue(added); });
afterEach(cleanup);

it("lets a personal workspace add the exact verified meeting with explicit consent", async () => {
  render(pane(context("personal", "acct-personal")));
  const add = await screen.findByRole("button", { name: copy.teams.addMeeting });
  expect(screen.getByText(copy.teams.personalNote)).toBeVisible();
  expect(calls.capture).not.toHaveBeenCalled();
  expect(calls.status.mock.calls[0][0]).toBe("token-personal-acct-personal");
  fireEvent.click(add);
  expect(await screen.findByText(copy.teams.added)).toBeVisible();
  expect(calls.capture).toHaveBeenCalledWith("token-personal-acct-personal", { meeting_id: "verified-meeting", chat_id: "sdk-chat" });
});

it("keeps the organization flow unchanged and shows no personal wording", async () => {
  render(pane(context("org", "acct-org")));
  fireEvent.click(await screen.findByRole("button", { name: copy.teams.addMeeting }));
  expect(await screen.findByText(copy.teams.added)).toBeVisible();
  expect(screen.queryByText(copy.teams.personalNote)).not.toBeInTheDocument();
  expect(calls.capture).toHaveBeenCalledWith("token-org-acct-org", { meeting_id: "verified-meeting", chat_id: "sdk-chat" });
});

it("aborts the old workspace's meeting read on a switch and ignores its late answer", async () => {
  let late!: (value: typeof ready) => void;
  calls.status.mockImplementationOnce(() => new Promise((resolve) => { late = resolve; })).mockResolvedValue({ ...ready, meeting_id: "personal-meeting", title: "Personal standup" });
  const view = render(pane(context("org", "acct-org")));
  await vi.waitFor(() => expect(calls.status).toHaveBeenCalledTimes(1));
  const signal = calls.status.mock.calls[0][2] as AbortSignal;
  view.rerender(pane(context("personal", "acct-personal")));
  expect(await screen.findByText("Personal standup")).toBeVisible();
  await act(async () => { late({ ...ready, title: "Org standup" }); });
  expect(signal.aborted).toBe(true);
  expect(screen.queryByText("Org standup")).not.toBeInTheDocument();
  expect(calls.status.mock.calls[1][0]).toBe("token-personal-acct-personal");
});

it("never sends the add if the workspace changes while the token is still being minted", async () => {
  let mint!: (token: string) => void;
  let reads = 0;
  const ctx = context("org", "acct-org", ((fn: (token: string) => Promise<unknown>) => {
    if (reads++ === 0) return fn("token-org");
    return new Promise((resolve, reject) => { mint = (token) => void Promise.resolve().then(() => fn(token)).then(resolve, reject); });
  }) as never);
  const view = render(pane(ctx));
  fireEvent.click(await screen.findByRole("button", { name: copy.teams.addMeeting }));
  view.rerender(pane(context("personal", "acct-personal")));
  await act(async () => { mint("stale-org-token"); });
  expect(calls.capture).not.toHaveBeenCalled();
});

it("states the provider limit when Microsoft is a consumer account, with no add action", async () => {
  calls.status.mockRejectedValue(new ApiError("refused", 403, "consumer_identity_unsupported"));
  render(pane(context("personal", "acct-personal")));
  expect(await screen.findByText(copy.meeting.explain.consumer_identity_unsupported)).toBeVisible();
  expect(screen.queryByRole("button", { name: copy.teams.addMeeting })).not.toBeInTheDocument();
  expect(screen.queryByRole("button", { name: copy.retry })).not.toBeInTheDocument();
});

it("shows the same provider limit if the refusal arrives when adding", async () => {
  calls.capture.mockRejectedValue(new ApiError("refused", 403, "consumer_identity_unsupported"));
  render(pane(context("personal", "acct-personal")));
  fireEvent.click(await screen.findByRole("button", { name: copy.teams.addMeeting }));
  expect(await screen.findByText(copy.meeting.explain.consumer_identity_unsupported)).toBeVisible();
});

it("shows only the generic failure for an unrecognized refusal", async () => {
  calls.capture.mockRejectedValue(new ApiError("organization_required internal", 403, "organization_required"));
  render(pane(context("personal", "acct-personal")));
  fireEvent.click(await screen.findByRole("button", { name: copy.teams.addMeeting }));
  expect(await screen.findByText(copy.teams.addFailed)).toBeVisible();
  expect(screen.queryByText(/organization_required|internal/)).not.toBeInTheDocument();
});

it("keeps the Microsoft log-in path for a personal workspace that is not connected yet", async () => {
  calls.status.mockRejectedValue(new ApiError("connect", 403, "connect_required"));
  render(pane(context("personal", "acct-personal")));
  expect(await screen.findByRole("button", { name: copy.teams.logIn })).toBeVisible();
  expect(calls.capture).not.toHaveBeenCalled();
});
