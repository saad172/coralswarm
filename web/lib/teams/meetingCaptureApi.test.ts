import { afterEach, expect, it, vi } from "vitest";
import { api, setTokenRefresher } from "../api";
import { guardedFetch } from "../transport";
vi.mock("../transport", async () => ({...(await vi.importActual<typeof import("../transport")>("../transport")), guardedFetch:vi.fn()}));
afterEach(()=>{vi.clearAllMocks();setTokenRefresher(null);});
it("sends one context-bound consent request and never retries with another workspace's bearer",async()=>{
  const refreshed=vi.fn().mockResolvedValue("new-workspace-token");setTokenRefresher(refreshed);
  vi.mocked(guardedFetch).mockResolvedValue(new Response(JSON.stringify({error:"Session expired"}),{status:401,headers:{"content-type":"application/json"}}));
  await expect(api.msTeams.captureMeeting("original-member-token",{meeting_ticket:"opaque"})).rejects.toThrow();
  expect(guardedFetch).toHaveBeenCalledTimes(1);expect(refreshed).not.toHaveBeenCalled();
  const [path,init]=vi.mocked(guardedFetch).mock.calls[0];
  expect(path).toBe("/v1/connectors/microsoft/teams/transcripts/capture");
  expect(init?.body).toBe(JSON.stringify({meeting_ticket:"opaque"}));
  expect(init?.headers).toMatchObject({Authorization:"Bearer original-member-token"});
});
it("reads owned capture status with an abort signal and preserves the member bearer",async()=>{
  const signal=new AbortController().signal;
  vi.mocked(guardedFetch).mockResolvedValue(new Response(JSON.stringify({meeting_id:"verified",capture:null}),{status:200,headers:{"content-type":"application/json"}}));
  await api.msTeams.meetingStatus("member-token",{capture_id:"owned-uuid"},signal);
  const [path,init]=vi.mocked(guardedFetch).mock.calls[0];
  expect(path).toBe("/v1/connectors/microsoft/teams/transcripts/meeting-status");
  expect(init?.method).toBe("POST"); expect(init?.body).toBe(JSON.stringify({capture_id:"owned-uuid"}));
  expect(init?.signal).toBe(signal); expect(init?.headers).toMatchObject({Authorization:"Bearer member-token"});
});

it("revokes exactly an owned capture ID and never retries with another workspace bearer",async()=>{
  const refreshed=vi.fn().mockResolvedValue("other-workspace-token");setTokenRefresher(refreshed);
  vi.mocked(guardedFetch).mockResolvedValue(new Response(JSON.stringify({error:"expired"}),{status:401,headers:{"content-type":"application/json"}}));
  await expect(api.msTeams.stopMeetingSelection("member-token","owned-selection")).rejects.toThrow();
  expect(guardedFetch).toHaveBeenCalledTimes(1);expect(refreshed).not.toHaveBeenCalled();
  const [path,init]=vi.mocked(guardedFetch).mock.calls[0];
  expect(path).toBe("/v1/connectors/microsoft/teams/transcripts/capture");expect(init?.method).toBe("DELETE");
  expect(init?.body).toBe(JSON.stringify({capture_id:"owned-selection"}));
});
