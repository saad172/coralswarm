// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { EmbeddedMeetingCapture } from "./EmbeddedMeetingCapture";
const state=vi.hoisted(()=>({signedIn:false,orgId:"org-a" as string|null,ready:true,resolvedOrgId:"org-a" as string|null,personal:false,setActive:vi.fn(),getToken:vi.fn(),status:vi.fn(),capture:vi.fn(),stop:vi.fn()}));
vi.mock("@clerk/nextjs",()=>({
  SignIn:({oauthFlow}: {oauthFlow:string})=> <div role="region" aria-label="Inline sign-in" data-oauth-flow={oauthFlow} />,
  useAuth:()=>({isLoaded:true,isSignedIn:state.signedIn,userId:"member",sessionId:"session",orgId:state.orgId,getToken:state.getToken}),
  useUser:()=>({user:{primaryEmailAddress:{emailAddress:"qa@example.test"}}}),
  useOrganization:()=>({organization:{name:"Workspace A"}}),
  useOrganizationList:()=>({isLoaded:true,setActive:state.setActive,userMemberships:{hasNextPage:false,isFetching:false,data:[{organization:{id:"org-a",name:"Workspace A"}},{organization:{id:"org-b",name:"Workspace B"}}]}}),
}));
vi.mock("@/lib/providers",()=>({useApp:()=>({accountId:"account",ready:state.ready,authReady:true,resolvedOrgId:state.resolvedOrgId,isPersonalOrg:state.personal,bootError:null,retryBoot:vi.fn()})}));
vi.mock("@/lib/api",async()=>({...await vi.importActual<typeof import("@/lib/api")>("@/lib/api"),api:{msTeams:{meetingStatus:state.status,captureMeeting:state.capture,stopMeetingSelection:state.stop}}}));
const panel=()=> <EmbeddedMeetingCapture selector={{join_url:"https://teams.microsoft.com/meet/123"}} connectionReturnPath="/outlook/meeting/connected" openAuthentication={vi.fn()} />;
beforeEach(()=>{vi.clearAllMocks();state.signedIn=false;state.orgId="org-a";state.resolvedOrgId="org-a";state.ready=true;state.personal=false;state.getToken.mockResolvedValue("fresh-member-token");state.status.mockResolvedValue({connection_required:false,availability_window_open:true,meeting_id:"verified",title:"QA",capture:null});});
afterEach(cleanup);
it("keeps sign-in inline and starts no meeting lookup while signed out",()=>{
  render(panel()); expect(screen.getByRole("region",{name:"Inline sign-in"})).toBeVisible();expect(screen.getByRole("region",{name:"Inline sign-in"})).toHaveAttribute("data-oauth-flow","popup");expect(state.status).not.toHaveBeenCalled();
});
it("removes current actions during workspace switch and waits for matching backend identity",async()=>{
  state.signedIn=true;const view=render(panel());await screen.findByRole("button",{name:"Enable capture"});
  let finish!:()=>void;state.setActive.mockImplementationOnce(()=>new Promise<void>(r=>{finish=r;}));
  fireEvent.change(screen.getByRole("combobox",{name:"Workspace"}),{target:{value:"org-b"}});
  expect(screen.queryByRole("button",{name:"Enable capture"})).not.toBeInTheDocument();
  expect(state.setActive).toHaveBeenCalledWith({organization:"org-b"});
  state.orgId="org-b";view.rerender(panel());await act(async()=>finish());
  expect(screen.getByText("Checking your workspace…")).toBeVisible();expect(state.capture).not.toHaveBeenCalled();
});
it("offers explicit consent in the personal workspace and returns to inline auth after session expiry",async()=>{
  state.signedIn=true;state.orgId=null;state.resolvedOrgId=null;const view=render(panel());
  expect(await screen.findByRole("button",{name:"Enable capture"})).toBeEnabled();
  expect(state.status).toHaveBeenCalledWith("fresh-member-token",{join_url:"https://teams.microsoft.com/meet/123"},expect.any(AbortSignal));
  expect(state.capture).not.toHaveBeenCalled();
  state.signedIn=false;view.rerender(panel());expect(screen.getByRole("region",{name:"Inline sign-in"})).toBeVisible();
});
