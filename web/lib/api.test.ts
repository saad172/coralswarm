// Regression test for the /v1/teams/invite 422: every caller in this file
// constructed its own payload, so the backend's invalid-role/valid-shape
// tests (backend/src/routes/teams.rs) never exercised what the client
// actually sends over the wire. Assert on the SERIALIZED fetch body, not on
// the arguments passed to `api.teams.invite`, so a regression that drops
// `role` again fails here even if the call site itself looks fine.
import { afterEach, describe, expect, it, vi } from "vitest";
import { api, type Graph } from "./api";
import { resolveTeamId } from "./teamId";

function mockFetchOk(body: unknown = {}) {
  const fetchMock = vi.fn().mockResolvedValue({
    ok: true,
    status: 200,
    json: async () => body,
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("api.teams.invite", () => {
  it("defaults role to 'member' and includes it alongside email in the request body", async () => {
    const fetchMock = mockFetchOk();
    await api.teams.invite("tok", "teammate@company.com");

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(typeof init.body).toBe("string");
    const sentBody = JSON.parse(init.body as string);
    expect(sentBody).toEqual({ email: "teammate@company.com", role: "member" });
  });

  it("sends an explicit role when one is passed", async () => {
    const fetchMock = mockFetchOk();
    await api.teams.invite("tok", "owner@company.com", "owner");

    const [, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    const sentBody = JSON.parse(init.body as string);
    expect(sentBody).toEqual({ email: "owner@company.com", role: "owner" });
  });
});

// Regression test for the "workspace Members panel shows 'team not found'
// for every shared workspace" production bug: `WorkspaceMembers.tsx` used to
// call `api.teams.roster(token, graph.id)` — the OCEAN id — but
// `GET /v1/teams/{team_id}/members` is keyed on `teams.id`, a DIFFERENT
// primary key. Assert on the ACTUAL URL `fetch` receives (not just on the
// argument handed to `api.teams.roster`), driven through `resolveTeamId` the
// same way the real call site is, so a regression that goes back to reading
// `graph.id` — anywhere in that chain — fails here.
describe("api.teams.roster / events (workspace-members team-id fix)", () => {
  const graph: Graph = {
    id: "ocean-11111111-1111-1111-1111-111111111111",
    name: "Engineering",
    kind: "team",
    is_primary: false,
    created_at: "2026-01-01T00:00:00Z",
    team_id: "team-22222222-2222-2222-2222-222222222222",
    my_team_role: "admin",
  };

  it("roster() hits the URL built from the team id, never the ocean id", async () => {
    const fetchMock = mockFetchOk([]);
    const teamId = resolveTeamId(graph);
    expect(teamId).toBe(graph.team_id);
    await api.teams.roster("tok", teamId as string);

    const [url] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toContain(`/v1/teams/${graph.team_id}/members`);
    expect(url).not.toContain(graph.id);
  });

  it("events() hits the URL built from the team id, never the ocean id", async () => {
    const fetchMock = mockFetchOk([]);
    const teamId = resolveTeamId(graph);
    await api.teams.events("tok", teamId as string, 1, 20);

    const [url] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toContain(`/v1/teams/${graph.team_id}/events`);
    expect(url).not.toContain(graph.id);
  });
});

// The manage page (app/(app)/workspaces/manage/page.tsx) is the first thing
// that puts every WRITE on this surface — rename, add member, change role,
// remove/leave — behind one screen, all routed through the exact same
// `resolveTeamId(graph)` chokepoint the roster/events reads above use. Each
// of these previously had no wire-level test at all (only the roster/events
// GETs did); this is "test the seam" for the mutating half of the same bug
// class — a call site that quietly went back to `graph.id` here would 404
// "team not found" in production exactly like the original panel did, and
// pass every other check (compiles, `resolveTeamId` itself is still correct)
// because nothing exercises the URL an actual mutation hits.
describe("api.teams write calls (rename / addMember / updateMemberRole / removeMember / leave) — built from team id, never the ocean id", () => {
  const graph: Graph = {
    id: "ocean-33333333-3333-3333-3333-333333333333",
    name: "Design",
    kind: "team",
    is_primary: false,
    created_at: "2026-01-01T00:00:00Z",
    team_id: "team-44444444-4444-4444-4444-444444444444",
    my_team_role: "admin",
  };
  const teamId = resolveTeamId(graph) as string;
  const userId = "user-55555555-5555-5555-5555-555555555555";

  it("rename() PATCHes /v1/teams/{team_id}, not /v1/teams/{ocean_id}", async () => {
    const fetchMock = mockFetchOk({ team_id: teamId, name: "New name", changed: true });
    await api.teams.rename("tok", teamId, "New name");

    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toContain(`/v1/teams/${teamId}`);
    expect(url).not.toContain(graph.id);
    expect(init.method).toBe("PATCH");
  });

  it("addMember() POSTs to /v1/teams/{team_id}/members, not the ocean id", async () => {
    const fetchMock = mockFetchOk({ user_id: userId, role: "member" });
    await api.teams.addMember("tok", teamId, userId, "member");

    const [url] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toContain(`/v1/teams/${teamId}/members`);
    expect(url).not.toContain(graph.id);
  });

  it("updateMemberRole() PATCHes /v1/teams/{team_id}/members/{user_id}, not the ocean id", async () => {
    const fetchMock = mockFetchOk({ user_id: userId, role: "admin", changed: true });
    await api.teams.updateMemberRole("tok", teamId, userId, "admin");

    const [url] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toContain(`/v1/teams/${teamId}/members/${userId}`);
    expect(url).not.toContain(graph.id);
  });

  it("removeMember() DELETEs /v1/teams/{team_id}/members/{user_id}, not the ocean id", async () => {
    const fetchMock = mockFetchOk();
    await api.teams.removeMember("tok", teamId, userId);

    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toContain(`/v1/teams/${teamId}/members/${userId}`);
    expect(url).not.toContain(graph.id);
    expect(init.method).toBe("DELETE");
  });

  it("leave() hits the same team-id-keyed route as removeMember (self-removal, same wire shape)", async () => {
    const fetchMock = mockFetchOk();
    await api.teams.leave("tok", teamId, userId);

    const [url] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toContain(`/v1/teams/${teamId}/members/${userId}`);
    expect(url).not.toContain(graph.id);
  });
});

// Archive/restore (workspace-manage design doc, "Archive and restore") are
// the newest additions to this same call chain — `ArchiveWorkspaceModal` and
// the archived-workspaces section on `/workspaces` both go through
// `resolveTeamId(graph)` exactly like every other mutation above, and this
// stack has already shipped the "sent the ocean id where a team id was
// wanted" bug once. Same "test the seam" reasoning: assert on the actual URL
// `fetch` receives, not on what's passed to `api.teams.archive`/`restore`.
describe("api.teams.archive / restore / listArchived — built from team id, never the ocean id", () => {
  const graph: Graph = {
    id: "ocean-66666666-6666-6666-6666-666666666666",
    name: "Platform",
    kind: "team",
    is_primary: false,
    created_at: "2026-01-01T00:00:00Z",
    team_id: "team-77777777-7777-7777-7777-777777777777",
    my_team_role: "admin",
  };
  const teamId = resolveTeamId(graph) as string;

  it("archive() DELETEs /v1/teams/{team_id}, not /v1/teams/{ocean_id}", async () => {
    const fetchMock = mockFetchOk();
    await api.teams.archive("tok", teamId);

    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toContain(`/v1/teams/${teamId}`);
    expect(url).not.toContain(graph.id);
    expect(init.method).toBe("DELETE");
  });

  it("restore() POSTs /v1/teams/{team_id}/restore, not /v1/teams/{ocean_id}/restore", async () => {
    const fetchMock = mockFetchOk({ team_id: teamId, archived: false, changed: true });
    await api.teams.restore("tok", teamId);

    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toContain(`/v1/teams/${teamId}/restore`);
    expect(url).not.toContain(graph.id);
    expect(init.method).toBe("POST");
  });

  it("listArchived() GETs the fixed /v1/teams/archived collection route, never a per-team URL", async () => {
    const fetchMock = mockFetchOk([]);
    await api.teams.listArchived("tok");

    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit | undefined];
    expect(url).toContain("/v1/teams/archived");
    expect(url).not.toContain(teamId);
    expect(url).not.toContain(graph.id);
    expect(init?.method ?? "GET").toBe("GET");
  });

  it("restore() surfaces a 409 (expired restore window) as an ApiError with status 409, not a thrown parse error", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: false,
      status: 409,
      json: async () => ({ error: "the 120-day restore window for this workspace has expired" }),
    });
    vi.stubGlobal("fetch", fetchMock);

    await expect(api.teams.restore("tok", teamId)).rejects.toMatchObject({
      status: 409,
      message: "the 120-day restore window for this workspace has expired",
    });
  });
});
