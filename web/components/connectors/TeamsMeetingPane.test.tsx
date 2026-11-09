// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi, type Mock } from "vitest";
import { app, authentication, meeting, pages } from "@microsoft/teams-js";
import { TeamsMeetingPane } from "./TeamsMeetingPane";
import { api, ApiError } from "@/lib/api";
// teams-js types getAuthToken with a callback overload; the SSO path uses the promise form.
const getAuthToken = authentication.getAuthToken as unknown as Mock;
vi.mock("./EmbeddedMeetingCapture",()=>({EmbeddedMeetingCapture:({selector}: {selector:unknown})=><div role="region" aria-label="Embedded capture">{JSON.stringify(selector)}</div>}));
vi.mock("@microsoft/teams-js",()=>({app:{initialize:vi.fn(),getContext:vi.fn(),openLink:vi.fn()},authentication:{getAuthToken:vi.fn()},meeting:{getMeetingDetails:vi.fn()},pages:{config:{registerOnSaveHandler:vi.fn(),setConfig:vi.fn(),setValidityState:vi.fn()}}}));
const status = vi.hoisted(() => vi.fn());
vi.mock("@/lib/api", async () => ({ ...(await vi.importActual<typeof import("@/lib/api")>("@/lib/api")), api: { msTeams: { meetingStatus: status, captureMeeting: vi.fn(), stopMeetingSelection: vi.fn(), connectMeeting: vi.fn() } } }));
beforeEach(()=>{
  vi.clearAllMocks(); vi.mocked(app.initialize).mockResolvedValue();vi.mocked(app.getContext).mockResolvedValue({meeting:{id:"teams-not-graph"},chat:{id:"chat-id"}} as Awaited<ReturnType<typeof app.getContext>>);
  vi.mocked(app.openLink).mockResolvedValue();
  // Default: Teams cannot issue an SSO token, so the existing Clerk flow runs (the legacy tests below).
  getAuthToken.mockRejectedValue(new Error("sso unavailable"));
  status.mockResolvedValue({ connection_required: false, availability_window_open: true, meeting_id: "verified", title: "Budget review", occurrence_start: "", occurrence_end: "", capture: null, recording_active: true });
});
afterEach(() => { cleanup(); vi.unstubAllGlobals(); });
it("keeps capture inside the meeting pane with SDK selectors and no browser handoff",async()=>{
  render(<TeamsMeetingPane />);
  expect(await screen.findByRole("region",{name:"Embedded capture"})).toHaveTextContent(JSON.stringify({teams_meeting_id:"teams-not-graph",chat_id:"chat-id"}));
  expect(app.openLink).not.toHaveBeenCalled(); expect(meeting.getMeetingDetails).not.toHaveBeenCalled();
});
it("refuses missing chat context instead of guessing Graph meeting identity",async()=>{
  vi.mocked(app.getContext).mockResolvedValue({meeting:{id:"not-graph"}} as Awaited<ReturnType<typeof app.getContext>>);
  render(<TeamsMeetingPane />);await screen.findByRole("alert");
  expect(app.openLink).not.toHaveBeenCalled();expect(screen.queryByRole("button")).not.toBeInTheDocument();
});
it("configuration itself does not call the capture API and discloses bundled bot intent",async()=>{
  render(<TeamsMeetingPane configure />);await screen.findByText("Adding CoralSwarm lets you save this meeting’s transcript to your workspace.");
  await vi.waitFor(()=>expect(pages.config.setValidityState).toHaveBeenCalledWith(true));
  expect(pages.config.registerOnSaveHandler).toHaveBeenCalled();
  expect(app.getContext).not.toHaveBeenCalled();
  expect(app.openLink).not.toHaveBeenCalled();
  expect(screen.queryByRole("status")).not.toBeInTheDocument();
  expect(screen.queryByRole("button",{name:"Open CoralSwarm to enable capture"})).not.toBeInTheDocument();
});
it("registers configuration save without resolving meeting or chat identifiers",async()=>{
  vi.mocked(app.getContext).mockRejectedValue(new Error("should not resolve meeting context"));
  render(<TeamsMeetingPane configure />);
  await vi.waitFor(()=>expect(pages.config.setValidityState).toHaveBeenCalledWith(true));
  expect(pages.config.registerOnSaveHandler).toHaveBeenCalled();
  expect(app.getContext).not.toHaveBeenCalled();
  expect(screen.queryByRole("alert")).not.toBeInTheDocument();
});

it("shows an actionable error when Teams SDK initialization is unavailable",async()=>{
  vi.mocked(app.initialize).mockRejectedValue(new Error("Open the scheduled meeting in Teams."));
  render(<TeamsMeetingPane />);
  expect(await screen.findByRole("alert")).toHaveTextContent("Open the scheduled meeting in Teams.");
  expect(app.openLink).not.toHaveBeenCalled();expect(meeting.getMeetingDetails).not.toHaveBeenCalled();
});

const ok = (body: unknown, code = 200) => new Response(JSON.stringify(body), { status: code });
const connected = (kind: "org" | "personal") => ({ status: "connected", token: "cs-token", expires_at: "2099-01-01T00:00:00Z", account: { email: "ada@acme.test" }, workspace: { account_id: "a", name: kind === "org" ? "Acme ISD" : "Personal", kind } });

const capture = { capture_id: "cap-1", status: "active" as const, last_checked_at: null, last_retrieved_at: null, retrieved_transcript_count: 0, last_error_code: null };
const ssoSession = (kind: "org" | "personal" = "org") => { getAuthToken.mockResolvedValue("teams-sso-token"); const fetchMock = vi.fn().mockResolvedValue(ok(connected(kind))); vi.stubGlobal("fetch", fetchMock); return fetchMock; };
const captureMeeting = vi.mocked(api.msTeams.captureMeeting);
const readyStatus = { connection_required: false, availability_window_open: true, meeting_id: "verified", title: "Budget review", occurrence_start: "", occurrence_end: "", capture: null, recording_active: true };
const offStatus = { ...readyStatus, recording_active: false };

it("SSO: connects from the Teams token, then shows only the meeting and Add meeting (no status, no account row, no menu)",async()=>{
  const fetchMock = ssoSession();
  render(<TeamsMeetingPane />);
  expect(await screen.findByRole("button",{name:"Add meeting"})).toBeVisible();
  expect(screen.getByText("Budget review")).toBeVisible();
  expect(screen.queryByRole("region",{name:"Embedded capture"})).not.toBeInTheDocument();
  for (const gone of [/Not saved yet/, /Connected/, /Check again/, /Stop/, /Change workspace/]) expect(screen.queryByText(gone)).not.toBeInTheDocument();
  expect(screen.queryByRole("button",{name:"More"})).not.toBeInTheDocument();
  expect(JSON.parse(String(fetchMock.mock.calls[0][1].body))).toEqual({ ms_token: "teams-sso-token" });
  expect(String(fetchMock.mock.calls[0][0])).toContain("/v1/addin/microsoft/session");
  expect(status.mock.calls[0][0]).toBe("cs-token"); expect(status.mock.calls[0][1]).toEqual({teams_meeting_id:"teams-not-graph",chat_id:"chat-id"});
  expect(status).toHaveBeenCalledTimes(1);
});
it("Ready is one hero card: title and the Add meeting button inside it, no date/time line",async()=>{
  ssoSession();
  const start = new Date(); start.setHours(14,0,0,0); const end = new Date(start); end.setMinutes(30);
  status.mockResolvedValue({ ...readyStatus, occurrence_start: start.toISOString(), occurrence_end: end.toISOString() });
  render(<TeamsMeetingPane />);
  const button = await screen.findByRole("button",{name:"Add meeting"});
  const title = screen.getByText("Budget review");
  expect(title.closest("section") as HTMLElement).toContainElement(button);
  expect(title).toHaveAttribute("title","Budget review");
  expect(screen.queryByText(/Today|Tomorrow|·/)).not.toBeInTheDocument();
});
it("Adding disables the in-card button",async()=>{
  ssoSession(); captureMeeting.mockReturnValue(new Promise(()=>{}));
  render(<TeamsMeetingPane />);
  fireEvent.click(await screen.findByRole("button",{name:"Add meeting"}));
  expect(await screen.findByRole("button",{name:"Adding…"})).toBeDisabled();
});
it("login-only state: with no link yet the pane shows one Log in button",async()=>{
  getAuthToken.mockResolvedValue("teams-sso-token");
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(ok({ status: "link_required", pairing: { code: "ABCD-1234", poll_secret: "s", approve_url: "https://app.coralswarm.example/addin/approve?c=1", expires_at: "2099-01-01T00:00:00Z" } })));
  render(<TeamsMeetingPane />);
  expect(await screen.findByRole("button",{name:"Log in"})).toBeVisible();
  expect(screen.getAllByRole("button")).toHaveLength(1);
  expect(status).not.toHaveBeenCalled();
});
it("first-time pairing stays inside the Log in card: code, Open approval page, no three-step screen",async()=>{
  getAuthToken.mockResolvedValue("teams-sso-token");
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(ok({ status: "link_required", pairing: { code: "ABCD-1234", poll_secret: "s", approve_url: "https://app.coralswarm.example/addin/approve?c=1", expires_at: "2099-01-01T00:00:00Z" } })));
  render(<TeamsMeetingPane />);
  fireEvent.click(await screen.findByRole("button",{name:"Log in"}));
  const open = await screen.findByRole("button",{name:"Open approval page"});
  expect(screen.getByTestId("pairing-code")).toHaveTextContent("ABCD-1234");
  expect(screen.getByText("Add your meeting to CoralSwarm").closest("section")).toContainElement(open);
  expect(screen.queryByText("Approve in your browser")).not.toBeInTheDocument();
  const secondary = screen.getByTestId("pairing-secondary"); // stacked, one line each, at ~320px
  expect(secondary.className).toContain("flex-col"); expect(secondary.className).toContain("whitespace-nowrap");
  expect(Array.from(secondary.querySelectorAll("button")).map((b) => b.textContent)).toEqual(["I've approved — continue", "Start over"]);
  expect(screen.queryByText("Tap Approve")).not.toBeInTheDocument();
});
it("Add meeting success shows the calm Added state and no buttons",async()=>{
  ssoSession(); captureMeeting.mockResolvedValue({ meeting_id: "verified", capture_id: "cap-1", status: "waiting_for_transcript" });
  render(<TeamsMeetingPane />);
  fireEvent.click(await screen.findByRole("button",{name:"Add meeting"}));
  expect(await screen.findByText("Added to CoralSwarm")).toBeVisible();
  expect(screen.getByText("Budget review")).toBeVisible(); // the title stays in the same card
  expect(captureMeeting).toHaveBeenCalledWith("cs-token",{ meeting_id: "verified", chat_id: "chat-id" });
  expect(screen.queryByRole("button")).not.toBeInTheDocument();
});
it("Add meeting failure shows a short message and the same button to retry",async()=>{
  ssoSession(); captureMeeting.mockRejectedValueOnce(new ApiError("boom",500,"boom")).mockResolvedValueOnce({ meeting_id: "verified", capture_id: "cap-1", status: "waiting_for_transcript" });
  render(<TeamsMeetingPane />);
  fireEvent.click(await screen.findByRole("button",{name:"Add meeting"}));
  expect(await screen.findByRole("alert")).toHaveTextContent("Couldn’t add this meeting. Try again.");
  fireEvent.click(screen.getByRole("button",{name:"Add meeting"}));
  expect(await screen.findByText("Added to CoralSwarm")).toBeVisible();
});
it("already-added meeting shows Added directly",async()=>{
  ssoSession(); status.mockResolvedValue({ connection_required: false, availability_window_open: true, meeting_id: "verified", title: "Budget review", occurrence_start: "", occurrence_end: "", capture });
  render(<TeamsMeetingPane />);
  expect(await screen.findByText("Added to CoralSwarm")).toBeVisible();
  expect(screen.queryByRole("button")).not.toBeInTheDocument();
});
it("Microsoft not connected: the same Log in button opens the Microsoft sign-in",async()=>{
  ssoSession(); status.mockResolvedValue({ connection_required: true, availability_window_open: true, meeting_id: "verified", title: null, occurrence_start: "", occurrence_end: "", capture: null });
  vi.mocked(api.msTeams.connectMeeting).mockResolvedValue({ authorization_url: "https://login.microsoftonline.com/common/oauth2/authorize?x=1" });
  render(<TeamsMeetingPane />);
  fireEvent.click(await screen.findByRole("button",{name:"Log in"}));
  await vi.waitFor(()=>expect(app.openLink).toHaveBeenCalledWith("https://login.microsoftonline.com/common/oauth2/authorize?x=1"));
  expect(screen.queryByText("Connect Microsoft")).not.toBeInTheDocument();
});
it("a connect_required answer from Add meeting returns to Log in",async()=>{
  ssoSession(); captureMeeting.mockRejectedValue(new ApiError("connect_required",409,"connect_required"));
  render(<TeamsMeetingPane />);
  fireEvent.click(await screen.findByRole("button",{name:"Add meeting"}));
  expect(await screen.findByRole("button",{name:"Log in"})).toBeVisible();
});
it("a status read failure shows a message and Try again",async()=>{
  ssoSession(); status.mockRejectedValueOnce(new ApiError("x",500));
  render(<TeamsMeetingPane />);
  expect(await screen.findByRole("alert")).toHaveTextContent("Couldn’t reach this meeting. Try again.");
  fireEvent.click(screen.getByRole("button",{name:"Try again"}));
  expect(await screen.findByRole("button",{name:"Add meeting"})).toBeVisible();
});
it("SSO: a personal workspace reaches the same explicit Add consent and adds nothing on open",async()=>{
  ssoSession("personal");
  render(<TeamsMeetingPane />);
  expect(await screen.findByRole("button",{name:"Add meeting"})).toBeVisible();
  expect(screen.getByText("Only you can see this transcript, in your personal workspace.")).toBeVisible();
  expect(status).toHaveBeenCalledTimes(1);
  expect(api.msTeams.captureMeeting).not.toHaveBeenCalled();
});
it("flag off (404 microsoft_sso_disabled): falls back to the existing Clerk pane unchanged",async()=>{
  getAuthToken.mockResolvedValue("teams-sso-token");
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(ok({ error: "microsoft_sso_disabled" }, 404)));
  render(<TeamsMeetingPane />);
  expect(await screen.findByRole("region",{name:"Embedded capture"})).toHaveTextContent(JSON.stringify({teams_meeting_id:"teams-not-graph",chat_id:"chat-id"}));
  expect(screen.queryByRole("button",{name:"Connect CoralSwarm"})).not.toBeInTheDocument();
});
it("SSO unavailable in Teams: falls back to the existing Clerk pane without calling the API",async()=>{
  const fetchMock = vi.fn(); vi.stubGlobal("fetch", fetchMock);
  render(<TeamsMeetingPane />);
  expect(await screen.findByRole("region",{name:"Embedded capture"})).toBeVisible();
  expect(fetchMock).not.toHaveBeenCalled();
});
it("before a meeting resolves, a connected pane shows the home state, not an error",async()=>{
  getAuthToken.mockResolvedValue("teams-sso-token");
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(ok(connected("org"))));
  vi.mocked(app.getContext).mockResolvedValue({} as Awaited<ReturnType<typeof app.getContext>>);
  render(<TeamsMeetingPane />);
  expect(await screen.findByText("Nothing from Teams yet")).toBeVisible();
  expect(await screen.findByRole("alert")).toHaveTextContent("Open CoralSwarm from a Teams meeting");
 
});

const OFF_LINE = "Turn on recording or transcription to add CoralSwarm";
const OFF_STEPS = "More (…) → Record and transcribe → Start transcription";
it("Off: shows the line and steps in the card, with no Add button",async()=>{
  ssoSession(); status.mockResolvedValue(offStatus);
  render(<TeamsMeetingPane />);
  const line = await screen.findByText(OFF_LINE);
  expect(screen.getByText(OFF_STEPS)).toBeVisible();
  expect(screen.getByText("Budget review").closest("section")).toContainElement(line);
  expect(screen.queryByRole("button")).not.toBeInTheDocument();
  expect(screen.queryByText("Turn on transcription")).not.toBeInTheDocument();
});
it("recording_active absent is treated as Off",async()=>{
  ssoSession(); const { recording_active: _omit, ...legacy } = readyStatus; void _omit; status.mockResolvedValue(legacy);
  render(<TeamsMeetingPane />);
  expect(await screen.findByText(OFF_LINE)).toBeVisible();
  expect(screen.queryByRole("button")).not.toBeInTheDocument();
});
it("Off polls every 5 s and flips to Ready when recording turns on",async()=>{
  vi.useFakeTimers({ shouldAdvanceTime: true });
  try {
    ssoSession(); status.mockResolvedValue(offStatus);
    render(<TeamsMeetingPane />);
    await screen.findByText(OFF_LINE);
    const before = status.mock.calls.length;
    status.mockResolvedValue(readyStatus);
    await act(async () => { await vi.advanceTimersByTimeAsync(5000); });
    expect(status.mock.calls.length).toBe(before + 1);
    expect(await screen.findByRole("button",{name:"Add meeting"})).toBeVisible();
    expect(screen.queryByText(OFF_LINE)).not.toBeInTheDocument();
  } finally { vi.useRealTimers(); }
});
it("polling stops when unmounted",async()=>{
  vi.useFakeTimers({ shouldAdvanceTime: true });
  try {
    ssoSession(); status.mockResolvedValue(offStatus);
    const view = render(<TeamsMeetingPane />);
    await screen.findByText(OFF_LINE);
    view.unmount();
    const before = status.mock.calls.length;
    await act(async () => { await vi.advanceTimersByTimeAsync(20000); });
    expect(status.mock.calls.length).toBe(before);
  } finally { vi.useRealTimers(); }
});
it("a 409 recording_off from Add returns to Off with no error text",async()=>{
  ssoSession(); captureMeeting.mockRejectedValue(new ApiError("recording_off",409,"recording_off"));
  render(<TeamsMeetingPane />);
  fireEvent.click(await screen.findByRole("button",{name:"Add meeting"}));
  expect(await screen.findByText(OFF_LINE)).toBeVisible();
  expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  expect(screen.queryByRole("button")).not.toBeInTheDocument();
});
it("Added shows only the check line: no time, hints or transcription prompt",async()=>{
  ssoSession(); status.mockResolvedValue({ ...readyStatus, capture, auto_transcription: "manual" });
  render(<TeamsMeetingPane />);
  expect(await screen.findByText("Added to CoralSwarm")).toBeVisible();
  for (const gone of [/Turn on transcription/, /Teams will record/, /Start transcription/, /saves the transcript/]) expect(screen.queryByText(gone)).not.toBeInTheDocument();
});
