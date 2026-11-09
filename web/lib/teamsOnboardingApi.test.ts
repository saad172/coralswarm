import { afterEach, expect, it, vi } from "vitest";
import { api, ApiError, setTokenRefresher } from "./api";
import { guardedFetch } from "./transport";
vi.mock("./transport", async () => ({
  ...(await vi.importActual<typeof import("./transport")>("./transport")),
  guardedFetch: vi.fn(),
}));
afterEach(() => { setTokenRefresher(null); vi.resetAllMocks(); });
it("does not replay enrollment using a refreshed bearer from a different workspace", async () => {
  const refresh = vi.fn(async () => "new-workspace-token");
  setTokenRefresher(refresh);
  vi.mocked(guardedFetch).mockResolvedValue(new Response(JSON.stringify({ error: "Expired session" }), { status: 401 }));
  await expect(api.msTeams.onboarding("original-workspace-token", "signed-ticket")).rejects.toBeInstanceOf(ApiError);
  expect(refresh).not.toHaveBeenCalled();
  expect(guardedFetch).toHaveBeenCalledTimes(1);
  const [path, init] = vi.mocked(guardedFetch).mock.calls[0];
  expect(path).toBe("/v1/connectors/microsoft/teams/onboarding");
  expect(init?.headers).toMatchObject({ Authorization: "Bearer original-workspace-token" });
  expect(init?.body).toBe(JSON.stringify({ ticket: "signed-ticket" }));
});

it("does not replay a targeted transcript pull under a refreshed workspace token", async () => {
  const refresh = vi.fn(async () => "other-workspace-token");
  setTokenRefresher(refresh);
  vi.mocked(guardedFetch).mockResolvedValue(new Response(JSON.stringify({ error: "Expired session" }), { status: 401 }));
  const target = { join_url: "https://teams.microsoft.com/l/meetup-join/isolated-fixture" };
  await expect(api.msTeams.syncMeeting("own-workspace-token", target)).rejects.toBeInstanceOf(ApiError);
  expect(refresh).not.toHaveBeenCalled();
  expect(guardedFetch).toHaveBeenCalledTimes(1);
  const [path, init] = vi.mocked(guardedFetch).mock.calls[0];
  expect(path).toBe("/v1/connectors/microsoft/teams/transcripts/sync-meeting");
  expect(init?.method).toBe("POST");
  expect(init?.headers).toMatchObject({ Authorization: "Bearer own-workspace-token" });
  expect(JSON.parse(String(init?.body))).toEqual(target);
});
