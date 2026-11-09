// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, fireEvent, screen } from "@testing-library/react";
import { render } from "@/lib/testing/queryRender";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { api, ApiError } from "@/lib/api";
import { MeetingTranscriptCapture } from "./MeetingTranscriptCapture";
const wire = vi.hoisted(() => ({ captures: vi.fn() }));
const context = vi.hoisted(() => ({
  query: new URLSearchParams("join_url=https%3A%2F%2Fteams.microsoft.com%2Fmeet%2F123"),
  app: { ready:true, authReady:true, accountId:"account-a", resolvedOrgId:"org-a" as string | null, isPersonalOrg:false, getAuthToken: () => `token-${context.auth.orgId ?? "personal"}` },
  auth:{isLoaded:true,userId:"user-a",sessionId:"session-a",orgId:"org-a" as string | null},
}));
vi.mock("next/navigation", () => ({useSearchParams:()=>context.query}));
vi.mock("@clerk/nextjs", () => ({useAuth:()=>context.auth,useOrganization:()=>({organization:{name:context.auth.orgId}})}));
vi.mock("@/lib/providers", () => ({useApp:()=>context.app}));
vi.mock("@/lib/api", async () => ({...(await vi.importActual<typeof import("@/lib/api")>("@/lib/api")), apiRequest:(path:string,token:string,_init:unknown,_retried:unknown,options?:{signal?:AbortSignal})=>{if(path!=="/v1/connectors/microsoft/teams/transcripts/captures")throw new Error(`Unexpected selection read ${path}`);return wire.captures(token,options?.signal);}, api:{msTeams:{connectMeeting:vi.fn(),meetingContext:vi.fn(),captureMeeting:vi.fn(),syncMeeting:vi.fn(),stopMeetingCapture:vi.fn()}}}));
const saved={capture_id:"cap-a",meeting_id:"verified-graph-id",created_at:"2026-10-01T00:00:00Z",status:"active" as const};
beforeEach(()=>{
  vi.resetAllMocks(); context.query = new URLSearchParams("join_url=https%3A%2F%2Fteams.microsoft.com%2Fmeet%2F123"); context.app.isPersonalOrg=false; context.auth.orgId="org-a";context.app.resolvedOrgId="org-a";context.app.accountId="account-a";
  wire.captures.mockResolvedValue({model:"teams_transcript_captures",version:1,computed_at:"2026-10-31T00:00:00Z",stale:false,refresh_eta:null,payload:{captures:[]}});
  vi.mocked(api.msTeams.captureMeeting).mockResolvedValue({capture_id:saved.capture_id,meeting_id:saved.meeting_id,status:"waiting_for_transcript"});
});
afterEach(cleanup);
it("does not enroll from a query, and explicitly saves consent without fetching",async()=>{
  render(<MeetingTranscriptCapture />);
  expect(await screen.findByText("org-a")).toBeInTheDocument();
  expect(screen.getByLabelText("Meeting join link")).toHaveValue("https://teams.microsoft.com/meet/123");
  expect(api.msTeams.captureMeeting).not.toHaveBeenCalled();
  wire.captures.mockResolvedValue({model:"teams_transcript_captures",version:1,computed_at:"2026-10-31T00:00:01Z",stale:false,refresh_eta:null,payload:{captures:[saved]}});
  fireEvent.click(screen.getByRole("button",{name:"Enable capture for this meeting"}));
  await screen.findByText(/nothing has been fetched/);
  expect(api.msTeams.captureMeeting).toHaveBeenCalledWith("token-org-a",{join_url:"https://teams.microsoft.com/meet/123"});
  expect(api.msTeams.syncMeeting).not.toHaveBeenCalled();
});
it("keeps consent visible on Graph denial and revokes only the verified saved meeting",async()=>{
  wire.captures.mockResolvedValue({model:"teams_transcript_captures",version:1,computed_at:"2026-10-31T00:00:01Z",stale:false,refresh_eta:null,payload:{captures:[saved]}});
  vi.mocked(api.msTeams.syncMeeting).mockRejectedValue(new Error("Microsoft blocked transcript access"));
  vi.mocked(api.msTeams.stopMeetingCapture).mockResolvedValue({status:"revoked"});
  render(<MeetingTranscriptCapture />);
  fireEvent.click(await screen.findByRole("button",{name:"Sync this meeting"}));
  await screen.findByText("Microsoft blocked transcript access");
  expect(screen.getByRole("button",{name:"Stop capture"})).toBeEnabled();
  fireEvent.click(screen.getByRole("button",{name:"Stop capture"}));
  await screen.findByText(/Future transcript capture stopped/);
  expect(api.msTeams.stopMeetingCapture).toHaveBeenCalledWith("token-org-a",saved.meeting_id);
});
it("does not show a previous member's in-flight result after workspace switch",async()=>{
  let finish!: (value: Awaited<ReturnType<typeof api.msTeams.captureMeeting>>) => void;
  vi.mocked(api.msTeams.captureMeeting).mockImplementationOnce(()=>new Promise(resolve=>{finish=resolve;}));
  const view=render(<MeetingTranscriptCapture />);
  fireEvent.click(screen.getByRole("button",{name:"Enable capture for this meeting"}));
  context.auth.orgId="org-b";context.app.resolvedOrgId="org-b"; view.rerender(<MeetingTranscriptCapture />);
  await act(async()=>finish({capture_id:"old",meeting_id:"old",status:"waiting_for_transcript"}));
  expect(screen.queryByText(/Capture enabled for this meeting only/)).not.toBeInTheDocument();
  expect(screen.getByText("org-b")).toBeInTheDocument();
});

it("resolves native opaque selectors without consent, then enrolls only the verified Graph meeting",async()=>{
  context.query=new URLSearchParams("teams_meeting_id=sdk-opaque&teams_chat_id=chat-opaque");
  vi.mocked(api.msTeams.meetingContext).mockResolvedValue({meeting_id:"verified-graph"});
  render(<MeetingTranscriptCapture />);
  await vi.waitFor(()=>expect(screen.getByRole("button",{name:"Enable capture for this meeting"})).toBeEnabled());
  expect(api.msTeams.meetingContext).toHaveBeenCalledWith("token-org-a",{teams_meeting_id:"sdk-opaque",chat_id:"chat-opaque"},expect.any(AbortSignal));
  expect(api.msTeams.captureMeeting).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button",{name:"Enable capture for this meeting"}));
  await vi.waitFor(()=>expect(api.msTeams.captureMeeting).toHaveBeenCalledWith("token-org-a",{meeting_id:"verified-graph"}));
});
function usePersonal(){ context.auth.orgId=null; context.app.resolvedOrgId=null; context.app.accountId="account-personal"; }
it("reaches explicit private consent from a personal workspace without enrolling on load",async()=>{
  usePersonal(); render(<MeetingTranscriptCapture />);
  expect(await screen.findByText("Personal workspace")).toBeInTheDocument();
  expect(screen.getByText(/private to you as the owner/)).toBeInTheDocument();
  expect(screen.getByText(/only does for work or school accounts/)).toBeInTheDocument();
  await vi.waitFor(()=>expect(wire.captures).toHaveBeenCalledWith("token-personal",expect.any(AbortSignal)));
  expect(api.msTeams.captureMeeting).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button",{name:"Enable capture for this meeting"}));
  await screen.findByText(/nothing has been fetched/);
  expect(api.msTeams.captureMeeting).toHaveBeenCalledWith("token-personal",{join_url:"https://teams.microsoft.com/meet/123"});
});
it("treats a personal organization as a personal workspace and still requires the explicit click",async()=>{
  context.app.isPersonalOrg=true; render(<MeetingTranscriptCapture />);
  expect(await screen.findByText("Personal workspace")).toBeInTheDocument();
  expect(api.msTeams.captureMeeting).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button",{name:"Enable capture for this meeting"}));
  await vi.waitFor(()=>expect(api.msTeams.captureMeeting).toHaveBeenCalledWith("token-org-a",{join_url:"https://teams.microsoft.com/meet/123"}));
});
it("fences an organization enrollment that finishes after switching to the personal workspace",async()=>{
  let finish!: (value: Awaited<ReturnType<typeof api.msTeams.captureMeeting>>) => void;
  vi.mocked(api.msTeams.captureMeeting).mockImplementationOnce(()=>new Promise(resolve=>{finish=resolve;}));
  wire.captures.mockImplementationOnce(()=>new Promise(()=>{})).mockResolvedValue({model:"teams_transcript_captures",version:1,computed_at:"2026-10-31T00:00:00Z",stale:false,refresh_eta:null,payload:{captures:[]}});
  const view=render(<MeetingTranscriptCapture />);
  const orgSignal=await vi.waitFor(()=>{ const s=wire.captures.mock.calls[0]?.[1]; expect(s).toBeDefined(); return s as AbortSignal; });
  fireEvent.click(screen.getByRole("button",{name:"Enable capture for this meeting"}));
  usePersonal(); view.rerender(<MeetingTranscriptCapture />);
  await act(async()=>finish({capture_id:"old",meeting_id:"old",status:"waiting_for_transcript"}));
  expect(orgSignal.aborted).toBe(true);
  expect(screen.queryByText(/Capture enabled for this meeting only/)).not.toBeInTheDocument();
  expect(screen.getByText("Personal workspace")).toBeInTheDocument();
  expect(screen.getByRole("button",{name:"Enable capture for this meeting"})).toBeEnabled();
  expect(api.msTeams.captureMeeting).toHaveBeenCalledTimes(1);
  expect(wire.captures).toHaveBeenLastCalledWith("token-personal",expect.any(AbortSignal));
});
it("cancels a pending meeting verification when the workspace changes and ignores its late answer",async()=>{
  context.query=new URLSearchParams("teams_meeting_id=sdk-opaque&teams_chat_id=chat-opaque");
  let late!: (value: {meeting_id:string}) => void;
  vi.mocked(api.msTeams.meetingContext).mockImplementationOnce(()=>new Promise(resolve=>{late=resolve;})).mockResolvedValue({meeting_id:"personal-verified"});
  const view=render(<MeetingTranscriptCapture />);
  const signal=await vi.waitFor(()=>{ const s=vi.mocked(api.msTeams.meetingContext).mock.calls[0]?.[2]; expect(s).toBeDefined(); return s as AbortSignal; });
  usePersonal(); view.rerender(<MeetingTranscriptCapture />);
  await vi.waitFor(()=>expect(screen.getByRole("button",{name:"Enable capture for this meeting"})).toBeEnabled());
  await act(async()=>late({meeting_id:"org-verified"}));
  expect(signal.aborted).toBe(true);
  fireEvent.click(screen.getByRole("button",{name:"Enable capture for this meeting"}));
  await vi.waitFor(()=>expect(api.msTeams.captureMeeting).toHaveBeenCalledWith("token-personal",{meeting_id:"personal-verified"}));
  expect(api.msTeams.captureMeeting).not.toHaveBeenCalledWith(expect.anything(),{meeting_id:"org-verified"});
});
it("states the provider limit for a consumer Microsoft account and never enables capture",async()=>{
  usePersonal();
  vi.mocked(api.msTeams.captureMeeting).mockRejectedValue(new ApiError("consumer",403,"consumer_identity_unsupported"));
  render(<MeetingTranscriptCapture />);
  fireEvent.click(screen.getByRole("button",{name:"Enable capture for this meeting"}));
  expect(await screen.findByText(/only shares meeting transcripts for work or school accounts/)).toBeInTheDocument();
  expect(screen.queryByRole("button",{name:"Connect Microsoft"})).not.toBeInTheDocument();
  expect(screen.queryByText(/Capture enabled for this meeting only/)).not.toBeInTheDocument();
});
it("keeps connect_required and identity mismatch answers working for a personal workspace",async()=>{
  usePersonal();
  vi.mocked(api.msTeams.captureMeeting).mockRejectedValueOnce(new ApiError("x",403,"microsoft_identity_mismatch")).mockRejectedValueOnce(new ApiError("x",403,"connect_required"));
  render(<MeetingTranscriptCapture />);
  fireEvent.click(screen.getByRole("button",{name:"Enable capture for this meeting"}));
  expect(await screen.findByText(/Use the Microsoft account that added CoralSwarm/)).toBeInTheDocument();
  fireEvent.click(screen.getByRole("button",{name:"Enable capture for this meeting"}));
  expect(await screen.findByRole("button",{name:"Connect Microsoft"})).toBeInTheDocument();
});
it("shows an unrecognized server answer generically, never its code or message",async()=>{
  usePersonal();
  vi.mocked(api.msTeams.captureMeeting).mockRejectedValue(new ApiError("organization_required internal detail",403,"organization_required"));
  render(<MeetingTranscriptCapture />);
  fireEvent.click(screen.getByRole("button",{name:"Enable capture for this meeting"}));
  expect(await screen.findByText("Unable to verify this meeting action.")).toBeInTheDocument();
  expect(screen.queryByText(/organization_required|internal detail/)).not.toBeInTheDocument();
});
it("keeps the signed meeting ticket opaque and requires explicit confirmation",async()=>{
  context.query=new URLSearchParams("meeting_ticket=signed-opaque&onboarding_result=active");
  render(<MeetingTranscriptCapture />);
  expect(api.msTeams.captureMeeting).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button",{name:"Enable capture for this meeting"}));
  await vi.waitFor(()=>expect(api.msTeams.captureMeeting).toHaveBeenCalledWith("token-org-a",{meeting_ticket:"signed-opaque"}));
});

it("fails closed on mixed meeting selections without resolving or enrolling either",()=>{
  context.query=new URLSearchParams("meeting_ticket=one&teams_meeting_id=two&teams_chat_id=chat");
  render(<MeetingTranscriptCapture />);
  expect(screen.getByText(/does not identify one meeting/)).toBeInTheDocument();
  expect(api.msTeams.meetingContext).not.toHaveBeenCalled();expect(api.msTeams.captureMeeting).not.toHaveBeenCalled();
});

it("preserves only the meeting selector for explicit OAuth and refuses an unrelated redirect",async()=>{
  context.query=new URLSearchParams("meeting_ticket=signed-opaque&onboarding_result=active");
  vi.mocked(api.msTeams.captureMeeting).mockRejectedValue(new ApiError("Connect first",403,"connect_required"));
  vi.mocked(api.msTeams.connectMeeting).mockResolvedValue({authorization_url:"https://evil.example/authorize"});
  render(<MeetingTranscriptCapture />);
  fireEvent.click(screen.getByRole("button",{name:"Enable capture for this meeting"}));
  fireEvent.click(await screen.findByRole("button",{name:"Connect Microsoft"}));
  await screen.findByText("Microsoft sign-in could not be verified.");
  expect(api.msTeams.connectMeeting).toHaveBeenCalledWith("token-org-a","/connectors/microsoft-teams/meeting-capture?meeting_ticket=signed-opaque");
  expect(api.msTeams.captureMeeting).toHaveBeenCalledTimes(1);
});
