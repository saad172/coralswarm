// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { ApiError } from "@/lib/api";
import { CurrentMeetingCapture, EmbeddedMeetingCapture } from "./EmbeddedMeetingCapture";
const calls = vi.hoisted(() => ({ status: vi.fn(), capture: vi.fn(), stop: vi.fn(), connect: vi.fn() }));
const shell = vi.hoisted(() => ({
  clerk: { isLoaded: true, isSignedIn: true, userId: "user-a", sessionId: "session-a", orgId: "org_a" as string | null },
  app: { accountId: "account-a", resolvedOrgId: "org_a" as string | null, ready: true, authReady: true, isPersonalOrg: false, bootError: false, retryBoot: () => {} },
  setActive: undefined as unknown as ReturnType<typeof import("vitest").vi.fn>,
  getToken: undefined as unknown as ReturnType<typeof import("vitest").vi.fn>,
}));
vi.mock("@/lib/api", async () => ({ ...(await vi.importActual<typeof import("@/lib/api")>("@/lib/api")), api: { msTeams: { meetingStatus: calls.status, captureMeeting: calls.capture, stopMeetingSelection: calls.stop, connectMeeting: calls.connect } } }));
vi.mock("@clerk/nextjs", () => ({
  SignIn: () => null,
  useAuth: () => ({ ...shell.clerk, getToken: shell.getToken }),
  useUser: () => ({ user: { primaryEmailAddress: { emailAddress: "owner@example.test" } } }),
  useOrganization: () => ({ organization: shell.clerk.orgId ? { name: "Org A" } : null }),
  useOrganizationList: () => ({ isLoaded: true, setActive: shell.setActive, userMemberships: { data: [{ organization: { id: "org_a", name: "Org A" } }, { organization: { id: "org_b", name: "Org B" } }], hasNextPage: false, isFetching: false, fetchNext: undefined } }),
}));
vi.mock("@/lib/providers", () => ({ useApp: () => shell.app }));
const token = vi.fn().mockResolvedValue("member-token");
const selector = { teams_meeting_id: "sdk-meeting", chat_id: "sdk-chat" };
const inactive = { connection_required: false, availability_window_open: true, meeting_id: "verified-only", title: "QA meeting", occurrence_start: "2026-10-01T10:00:00Z", occurrence_end: "2026-10-01T11:00:00Z", capture: null };
const active = { ...inactive, capture: { capture_id: "own-capture", status: "active" as const, last_checked_at: null, last_retrieved_at: null, retrieved_transcript_count: 0, last_error_code: null } };
function pane(identity = "member:workspace") { return <CurrentMeetingCapture key={identity} selector={selector} identity={identity} getToken={token} workspace="QA" openAuthentication={vi.fn()} connectionReturnPath="/teams/meeting/connected" />; }
beforeEach(() => {
  vi.clearAllMocks(); token.mockResolvedValue("member-token"); calls.status.mockResolvedValue(inactive);
  shell.clerk.orgId = "org_a"; shell.app.resolvedOrgId = "org_a"; shell.app.accountId = "account-a"; shell.app.isPersonalOrg = false;
  shell.setActive = vi.fn().mockResolvedValue(undefined);
  shell.getToken = vi.fn(async () => `token-${shell.clerk.orgId ?? "personal"}`);
});
afterEach(() => { cleanup(); vi.useRealTimers(); });
it("resolves read-only and only captures the backend verified current meeting on explicit click", async () => {
  calls.capture.mockResolvedValue({ meeting_id: "verified-only", capture_id: "new-capture", status: "waiting_for_transcript" });
  render(pane());
  fireEvent.click(await screen.findByRole("button", { name: "Enable capture" }));
  await vi.waitFor(() => expect(calls.capture).toHaveBeenCalledWith("member-token", { meeting_id: "verified-only" }));
  expect(calls.status.mock.calls[0][1]).toEqual(selector);
  expect(calls.stop).not.toHaveBeenCalled();
});
it("reopens active capture with Stop and never grants a second consent", async () => {
  calls.status.mockResolvedValue(active); calls.stop.mockResolvedValue({ status: "revoked" });
  render(pane());
  fireEvent.click(await screen.findByRole("button", { name: "Stop capture" }));
  await vi.waitFor(() => expect(calls.stop).toHaveBeenCalledWith("member-token", "own-capture"));
  expect(calls.capture).not.toHaveBeenCalled();
  expect(await screen.findByText(/Capture stopped. Transcripts already saved/)).toBeVisible();
});
it("polls an owned capture ID and updates provider status without manual Sync or repeated meeting resolution", async () => {
  vi.useFakeTimers(); calls.status.mockResolvedValueOnce(active).mockResolvedValue({ ...active, title: null, capture: { ...active.capture, last_checked_at: "2026-10-01T11:01:00Z", last_retrieved_at: "2026-10-01T11:01:00Z", retrieved_transcript_count: 1 } });
  render(pane()); await act(async () => { await vi.advanceTimersByTimeAsync(0); });
  expect(screen.getByText("Waiting for transcript")).toBeVisible();
  await act(async () => { await vi.advanceTimersByTimeAsync(15_000); });
  expect(calls.status.mock.calls[1][1]).toEqual({ capture_id: "own-capture" });
  expect(screen.getByText("Transcript retrieved")).toBeVisible(); expect(screen.getByText("QA meeting")).toBeVisible();
  expect(calls.capture).not.toHaveBeenCalled(); expect(calls.stop).not.toHaveBeenCalled();
});
it("does not repeatedly resolve Microsoft metadata while capture is off", async () => {
  vi.useFakeTimers(); render(pane()); await act(async () => { await vi.advanceTimersByTimeAsync(0); });
  await act(async () => { await vi.advanceTimersByTimeAsync(60_000); });
  expect(calls.status).toHaveBeenCalledTimes(1);
});
it("discards an old workspace response and aborts its status request", async () => {
  let resolve!: (value: typeof active) => void;
  calls.status.mockImplementationOnce(() => new Promise(r => { resolve = r; })).mockResolvedValue(inactive);
  const view = render(pane("member:old")); await vi.waitFor(() => expect(calls.status).toHaveBeenCalledTimes(1));
  const signal = calls.status.mock.calls[0][2] as AbortSignal;
  view.rerender(pane("member:new"));
  await screen.findByRole("button", { name: "Enable capture" });
  await act(async () => { resolve(active); });
  expect(signal.aborted).toBe(true); expect(screen.queryByRole("button", { name: "Stop capture" })).not.toBeInTheDocument();
});
it("does not send a write if the member changes while fresh token minting is pending", async () => {
  render(pane()); const enable = await screen.findByRole("button", { name: "Enable capture" });
  let resolve!: (value: string) => void; token.mockImplementationOnce(() => new Promise(r => { resolve = r; }));
  fireEvent.click(enable); cleanup();
  await act(async () => { resolve("other-workspace-token"); });
  expect(calls.capture).not.toHaveBeenCalled();
});
it("rejects a status poll that returns another meeting and removes actions until verified", async () => {
  vi.useFakeTimers(); calls.status.mockResolvedValueOnce(active).mockResolvedValue({ ...active, meeting_id: "another-meeting" });
  render(pane()); await act(async () => { await vi.advanceTimersByTimeAsync(0); });
  await act(async () => { await vi.advanceTimersByTimeAsync(15_000); });
  expect(screen.queryByRole("button", { name: "Stop capture" })).not.toBeInTheDocument();
  expect(screen.getByText("Capture status is unavailable.")).toBeVisible();
});

it("shows an ended capture window and preserves Stop without allowing a new Enable", async () => {
  calls.status.mockResolvedValue({ ...active, availability_window_open: false });
  render(pane());
  expect(await screen.findByText("Capture window ended")).toBeVisible();
  expect(screen.getByRole("button", { name: "Stop capture" })).toBeEnabled();
  cleanup(); calls.status.mockResolvedValue({ ...inactive, availability_window_open: false });
  render(pane());
  expect(await screen.findByRole("button", { name: "Enable capture" })).toBeDisabled();
});

it("keeps consent and Stop unavailable for malformed status responses", async () => {
  calls.status.mockResolvedValue({ ...active, capture: { ...active.capture, retrieved_transcript_count: -1 } });
  render(pane());
  expect(await screen.findByText("Capture status is unavailable.")).toBeVisible();
  expect(screen.queryByRole("button", { name: "Enable capture" })).not.toBeInTheDocument();
  expect(screen.queryByRole("button", { name: "Stop capture" })).not.toBeInTheDocument();
});

it("reports an empty successful provider check without claiming pending saved content", async () => {
  calls.status.mockResolvedValue({ ...active, capture: { ...active.capture, last_retrieved_at: "2026-10-01T11:01:00Z", retrieved_transcript_count: 0 } });
  render(pane());
  expect(await screen.findByText("Waiting for transcript")).toBeVisible();
  expect(screen.getByText("No transcript was available at the last successful check.")).toBeVisible();
  expect(screen.queryByText(/Processing into your private ocean/)).not.toBeInTheDocument();
});

it("keeps owner Stop available after Microsoft disconnect and never grants capture permission", async () => {
  calls.status.mockResolvedValue({ ...active, connection_required: true }); calls.stop.mockResolvedValue({ status: "revoked" });
  render(pane());
  const stop = await screen.findByRole("button", { name: "Stop capture" });
  expect(screen.getByRole("button", { name: "Connect Microsoft" })).toBeVisible();
  expect(stop).toBeEnabled(); fireEvent.click(stop);
  await vi.waitFor(() => expect(calls.stop).toHaveBeenCalledWith("member-token", "own-capture"));
  expect(calls.capture).not.toHaveBeenCalled();
});
it("requires reconnection before re-enabling a stopped owned selection", async () => {
  calls.status.mockResolvedValue({ ...active, connection_required: true, capture: { ...active.capture, status: "stopped" } });
  render(pane());
  expect(await screen.findByRole("button", { name: "Enable capture" })).toBeDisabled();
  expect(screen.getByRole("button", { name: "Connect Microsoft" })).toBeVisible();
});

const goPersonal = () => { shell.clerk.orgId = null; shell.app.resolvedOrgId = null; shell.app.accountId = "account-personal"; };
const workspacePicker = () => screen.getByRole("combobox", { name: "Workspace" }) as HTMLSelectElement;

it("reaches explicit private consent in a personal workspace and sends the owner's own token only on click", async () => {
  goPersonal(); calls.capture.mockResolvedValue({ meeting_id: "verified-only", capture_id: "new-capture", status: "waiting_for_transcript" });
  render(<EmbeddedMeetingCapture selector={selector} connectionReturnPath="/teams/meeting/connected" openAuthentication={vi.fn()} />);
  const enable = await screen.findByRole("button", { name: "Enable capture" });
  expect(workspacePicker().value).not.toBe(""); expect(workspacePicker().selectedOptions[0]).toHaveTextContent("Personal workspace");
  expect(screen.getByText(/visible only to you/)).toBeVisible(); expect(screen.getByText(/work or school accounts only/)).toBeVisible();
  expect(calls.status.mock.calls[0][0]).toBe("token-personal"); expect(calls.capture).not.toHaveBeenCalled();
  fireEvent.click(enable);
  await vi.waitFor(() => expect(calls.capture).toHaveBeenCalledWith("token-personal", { meeting_id: "verified-only" }));
});

it("offers a real Personal workspace choice that switches Clerk to no organization, not a string", async () => {
  render(<EmbeddedMeetingCapture selector={selector} connectionReturnPath="/teams/meeting/connected" openAuthentication={vi.fn()} />);
  await screen.findByRole("button", { name: "Enable capture" });
  expect(screen.queryByRole("option", { name: "Choose a workspace" })).not.toBeInTheDocument();
  expect(screen.getAllByRole("option").map((option) => option.textContent)).toEqual(["Personal workspace", "Org A", "Org B"]);
  fireEvent.change(workspacePicker(), { target: { value: screen.getByRole("option", { name: "Personal workspace" }).getAttribute("value") } });
  await vi.waitFor(() => expect(shell.setActive).toHaveBeenCalledTimes(1));
  expect(shell.setActive.mock.calls[0][0]).toStrictEqual({ organization: null });
  await screen.findByRole("button", { name: "Enable capture" });
  fireEvent.change(workspacePicker(), { target: { value: "org_b" } });
  await vi.waitFor(() => expect(shell.setActive).toHaveBeenCalledTimes(2));
  expect(shell.setActive.mock.calls[1][0]).toStrictEqual({ organization: "org_b" });
});

it("keeps the organization flow and wording unchanged", async () => {
  calls.capture.mockResolvedValue({ meeting_id: "verified-only", capture_id: "new-capture", status: "waiting_for_transcript" });
  render(<EmbeddedMeetingCapture selector={selector} connectionReturnPath="/teams/meeting/connected" openAuthentication={vi.fn()} />);
  fireEvent.click(await screen.findByRole("button", { name: "Enable capture" }));
  await vi.waitFor(() => expect(calls.capture).toHaveBeenCalledWith("token-org_a", { meeting_id: "verified-only" }));
  expect(screen.getByText(/member-owned private workspace in/)).toBeVisible();
  expect(screen.queryByText(/work or school accounts only/)).not.toBeInTheDocument();
});

it("unmounts and aborts the old workspace's read while Clerk switches, then discards its late answer", async () => {
  let resolve!: (value: typeof active) => void;
  calls.status.mockImplementationOnce(() => new Promise((r) => { resolve = r; })).mockResolvedValue(inactive);
  let finishSwitch!: () => void; shell.setActive.mockImplementation(() => new Promise<void>((r) => { finishSwitch = r; }));
  const ui = () => <EmbeddedMeetingCapture selector={selector} connectionReturnPath="/teams/meeting/connected" openAuthentication={vi.fn()} />;
  const view = render(ui()); await vi.waitFor(() => expect(calls.status).toHaveBeenCalledTimes(1));
  const signal = calls.status.mock.calls[0][2] as AbortSignal;
  fireEvent.change(workspacePicker(), { target: { value: screen.getByRole("option", { name: "Personal workspace" }).getAttribute("value") } });
  await screen.findByText("Checking your workspace…"); expect(signal.aborted).toBe(true);
  goPersonal(); await act(async () => { finishSwitch(); }); view.rerender(ui());
  await screen.findByRole("button", { name: "Enable capture" });
  await act(async () => { resolve(active); });
  expect(screen.queryByRole("button", { name: "Stop capture" })).not.toBeInTheDocument();
  expect(calls.status.mock.calls[1][0]).toBe("token-personal");
});

it("does not pair a personal pane with an organization token while the app still describes the old workspace", async () => {
  goPersonal(); shell.app.resolvedOrgId = "org_a"; shell.app.accountId = "account-a";
  render(<EmbeddedMeetingCapture selector={selector} connectionReturnPath="/teams/meeting/connected" openAuthentication={vi.fn()} />);
  expect(screen.getByText("Checking your workspace…")).toBeVisible();
  expect(calls.status).not.toHaveBeenCalled();
});

it("states the provider limit for a consumer Microsoft account instead of enabling capture", async () => {
  goPersonal(); calls.capture.mockRejectedValue(new ApiError("refused", 403, "consumer_identity_unsupported"));
  render(<EmbeddedMeetingCapture selector={selector} connectionReturnPath="/teams/meeting/connected" openAuthentication={vi.fn()} />);
  fireEvent.click(await screen.findByRole("button", { name: "Enable capture" }));
  expect(await screen.findByText(/only shares meeting transcripts for work or school accounts/)).toBeVisible();
  expect(screen.queryByText(/Capture enabled for this meeting/)).not.toBeInTheDocument();
});

it("shows an unrecognized refusal generically without its code", async () => {
  goPersonal(); calls.capture.mockRejectedValue(new ApiError("organization_required internal", 403, "organization_required"));
  render(<EmbeddedMeetingCapture selector={selector} connectionReturnPath="/teams/meeting/connected" openAuthentication={vi.fn()} />);
  fireEvent.click(await screen.findByRole("button", { name: "Enable capture" }));
  expect(await screen.findByText("This meeting could not be checked. Try again; capture has not changed.")).toBeVisible();
  expect(screen.queryByText(/organization_required|internal/)).not.toBeInTheDocument();
});
