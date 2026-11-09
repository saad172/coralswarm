import { oceansIndexQueryOptions } from "./oceansIndex";
import { partialMatchKey } from "@tanstack/query-core";
import { describe, expect, it, vi } from "vitest";
import { ApiError } from "./api";
import { TransportError } from "./transport";

const read = vi.hoisted(() => vi.fn());
vi.mock("./api", async (actual) => ({ ...(await actual<typeof import("./api")>()), apiRequest: read }));

import {
  conversationKey,
  conversationQueryOptions,
  prefetchConversation,
  activityQueryOptions,
  appInvalidation,
  archivedTeamsQueryOptions,
  conversationsQueryOptions,
  connectorQueryOptions,
  connectorsQueryOptions,
  invalidationFor,
  teamEventsQueryOptions,
  teamRosterQueryOptions,
  workspaceReefsQueryOptions,
  graphStatsQueryOptions,
  journalDayQueryOptions,
  prefetchDestination,
  usageSummaryQueryOptions,
  type QueryCtx,
} from "./appQueries";
import { makeQueryClient } from "./queryClient";
import { homeAttentionQueryOptions, homeContinueQueryOptions, homeWhatsNewQueryOptions } from "./homeLaunchpadApi";
import { formatDayParam, parseDayParam } from "./journal";

const ctx = (scope: string, token = "t"): QueryCtx => ({ getAuthToken: () => token, scope });

describe("app query keys", () => {
  it("carry the identity scope and never the bearer", () => {
    const a = conversationsQueryOptions(ctx("user-a::personal", "bearer-1")).queryKey;
    const rotated = conversationsQueryOptions(ctx("user-a::personal", "bearer-2")).queryKey;
    const otherOrg = conversationsQueryOptions(ctx("user-a::org_b", "bearer-1")).queryKey;
    // A rotation is the same entry; an identity switch is a different one.
    expect(rotated).toEqual(a);
    expect(otherOrg).not.toEqual(a);
    expect(JSON.stringify(a)).not.toContain("bearer");
  });

  it("put the scope second so a mutation can invalidate a resource without knowing it", () => {
    const key = journalDayQueryOptions(ctx("s"), "2026-10-20").queryKey;
    expect(key[1]).toBe("s");
    expect(partialMatchKey(key, appInvalidation.journal())).toBe(true);
    expect(partialMatchKey(usageSummaryQueryOptions(ctx("s")).queryKey, appInvalidation.usageSummary())).toBe(true);
  });

  it("file workspace stats under the workbench's ocean prefix", () => {
    const key = graphStatsQueryOptions(ctx("s"), "g1").queryKey;
    // An ingest's declared invalidation reaches the index card's counts.
    expect(invalidationFor.ingest("g1").some((prefix) => partialMatchKey(key, prefix))).toBe(true);
  });
});

describe("invalidationFor", () => {
  const reaches = (prefixes: readonly (readonly unknown[])[], key: readonly unknown[]) =>
    prefixes.some((prefix) => partialMatchKey(key, prefix as unknown[]));
  const c = ctx("s");

  it("archive and restore reach the Archived section, the roster and the audit trail", () => {
    for (const prefixes of [invalidationFor.workspaceArchived(), invalidationFor.workspaceRestored(), invalidationFor.workspaceRenamed(), invalidationFor.workspaceMembersChanged()]) {
      expect(reaches(prefixes, oceansIndexQueryOptions(c).queryKey)).toBe(true);
    }
    for (const prefixes of [invalidationFor.workspaceArchived(), invalidationFor.workspaceRestored()]) {
      expect(reaches(prefixes, archivedTeamsQueryOptions(c).queryKey)).toBe(true);
      expect(reaches(prefixes, teamRosterQueryOptions(c, "t1").queryKey)).toBe(true);
      expect(reaches(prefixes, teamEventsQueryOptions(c, "t1", 20).queryKey)).toBe(true);
    }
  });

  it("member changes reach the roster and audit trail; a rename reaches the audit trail", () => {
    expect(reaches(invalidationFor.workspaceMembersChanged(), teamRosterQueryOptions(c, "t1").queryKey)).toBe(true);
    expect(reaches(invalidationFor.workspaceMembersChanged(), teamEventsQueryOptions(c, "t1", 20).queryKey)).toBe(true);
    expect(reaches(invalidationFor.workspaceRenamed(), teamEventsQueryOptions(c, "t1", 20).queryKey)).toBe(true);
  });

  it("promote reaches every destination ocean and the shared feeds, not another ocean", () => {
    const prefixes = invalidationFor.promote(["g2", "g3"]);
    expect(reaches(prefixes, graphStatsQueryOptions(c, "g2").queryKey)).toBe(true);
    expect(reaches(prefixes, graphStatsQueryOptions(c, "g3").queryKey)).toBe(true);
    expect(reaches(prefixes, graphStatsQueryOptions(c, "g9").queryKey)).toBe(false);
    expect(reaches(prefixes, workspaceReefsQueryOptions(c, "g2", null, 24).queryKey)).toBe(true);
    expect(reaches(prefixes, activityQueryOptions(c, "g2").queryKey)).toBe(true);
    expect(reaches(prefixes, journalDayQueryOptions(c, "2026-10-20").queryKey)).toBe(true);
  });

  it("ingest reaches the ocean, its topic grid, activity and the journal", () => {
    const prefixes = invalidationFor.ingest("g1");
    expect(reaches(prefixes, graphStatsQueryOptions(c, "g1").queryKey)).toBe(true);
    expect(reaches(prefixes, workspaceReefsQueryOptions(c, "g1", null, 24).queryKey)).toBe(true);
    expect(reaches(prefixes, activityQueryOptions(c, null).queryKey)).toBe(true);
    expect(reaches(prefixes, journalDayQueryOptions(c, "2026-10-20").queryKey)).toBe(true);
  });

  // Home re-reads on every visit, so only a change that can land while Home is
  // on screen reaches its reads: the connector repair its Fix leads to, and a
  // chat deleted from the sidebar.
  it("reaches a Home launchpad read only from what can change while Home is open", () => {
    const home = {
      continue: homeContinueQueryOptions(c).queryKey,
      whatsNew: homeWhatsNewQueryOptions(c).queryKey,
      attention: homeAttentionQueryOptions(c).queryKey,
    };
    const reached = (prefixes: readonly (readonly unknown[])[]) =>
      (Object.keys(home) as (keyof typeof home)[]).filter((k) => reaches(prefixes, home[k]));
    expect(reached(invalidationFor.connectorChanged())).toEqual(["attention"]);
    expect(reached(invalidationFor.chatDeleted())).toEqual(["continue"]);
    for (const prefixes of [
      invalidationFor.workspaceRenamed(),
      invalidationFor.workspaceArchived(),
      invalidationFor.workspaceRestored(),
      invalidationFor.workspaceMembersChanged(),
      invalidationFor.ingest("g1"),
      invalidationFor.promote(["g2"]),
      invalidationFor.edgeReviewed("g1"),
    ]) {
      expect(reached(prefixes)).toEqual([]);
    }
  });

  it("a connector change still reaches the connector list and detail", () => {
    const prefixes = invalidationFor.connectorChanged();
    expect(reaches(prefixes, connectorsQueryOptions(c).queryKey)).toBe(true);
    expect(reaches(prefixes, connectorQueryOptions(c, "slack").queryKey)).toBe(true);
  });
});

describe("prefetchDestination", () => {
  it("warms exactly the keys the destinations read", async () => {
    const qc = makeQueryClient();
    const spy = vi.spyOn(qc, "prefetchQuery").mockResolvedValue(undefined);
    const c = { ...ctx("s"), authReady: true, activitiesEnabled: false, graphIds: ["g1", "g2"] };
    prefetchDestination(qc, "/today", c);
    prefetchDestination(qc, "/chats", c);
    prefetchDestination(qc, "/connectors", c);
    prefetchDestination(qc, "/workspaces", c);
    const warmed = spy.mock.calls.map(([options]) => JSON.stringify(options.queryKey));
    expect(warmed).toHaveLength(4);
    expect(warmed).toContain(JSON.stringify(journalDayQueryOptions(c, formatDayParam(parseDayParam(null))).queryKey));
    expect(warmed).toContain(JSON.stringify(conversationsQueryOptions(c).queryKey));
    expect(warmed).not.toContain(JSON.stringify(["oauthConnections", "s"]));
    expect(warmed).toContain(JSON.stringify(oceansIndexQueryOptions(c).queryKey));
    expect(warmed).not.toContain(JSON.stringify(graphStatsQueryOptions(c, "g2").queryKey));
  });

  // Home re-reads all three on mount, so a warm read would be a second one.
  it("warms nothing for Home", () => {
    const qc = makeQueryClient();
    const spy = vi.spyOn(qc, "prefetchQuery");
    prefetchDestination(qc, "/home", { ...ctx("s"), authReady: true, graphIds: ["g1"] });
    expect(spy).not.toHaveBeenCalled();
  });

  it("does nothing before auth is ready", () => {
    const qc = makeQueryClient();
    const spy = vi.spyOn(qc, "prefetchQuery");
    prefetchDestination(qc, "/chats", { ...ctx("s"), authReady: false, graphIds: [] });
    expect(spy).not.toHaveBeenCalled();
  });
});

describe("prefetchConversation", () => {
  const detail = (id: string) => ({ model: "conversation_detail", version: 1, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null, payload: { resolved_scope: null,
    conversation: { id, graph_id: "g", title: "Chat", pinned: false, shared: false, mine: true, created_at: "2026-10-26", updated_at: "2026-10-26", scope_status: "unscoped", library_scope: null },
    messages: [], messages_truncated: false, older_before_seq: null,
  } });
  const c = { ...ctx("s"), authReady: true };

  it("keys one chat under its identity scope, apart from the list", () => {
    expect(conversationQueryOptions(c, "11111111-1111-4111-8111-111111111111").queryKey).toEqual(conversationKey("s", "11111111-1111-4111-8111-111111111111"));
    expect(conversationQueryOptions(ctx("other"), "11111111-1111-4111-8111-111111111111").queryKey).not.toEqual(conversationKey("s", "11111111-1111-4111-8111-111111111111"));
    expect(partialMatchKey(conversationKey("s", "11111111-1111-4111-8111-111111111111"), appInvalidation.conversations())).toBe(false);
  });

  it("skips a fresh entry and reads an absent one", async () => {
    read.mockReset().mockResolvedValue(detail("22222222-2222-4222-8222-222222222222"));
    const qc = makeQueryClient();
    qc.setQueryData(conversationKey("s", "11111111-1111-4111-8111-111111111111"), detail("11111111-1111-4111-8111-111111111111"));
    prefetchConversation(qc, c, "11111111-1111-4111-8111-111111111111");
    prefetchConversation(qc, c, "22222222-2222-4222-8222-222222222222");
    await vi.waitFor(() => expect(qc.getQueryData(conversationKey("s", "22222222-2222-4222-8222-222222222222"))).toEqual(detail("22222222-2222-4222-8222-222222222222")));
    expect(read).toHaveBeenCalledTimes(1);
    expect(read).toHaveBeenCalledWith("/v1/load/conversations/22222222-2222-4222-8222-222222222222", "t", { cache: "no-store" }, false, expect.objectContaining({ priority: "background", signal: expect.any(AbortSignal) }));
  });

  it("a refused warm drops the entry's thread and is not re-asked", async () => {
    read.mockReset().mockRejectedValue(new ApiError("gone", 403));
    const qc = makeQueryClient();
    qc.setQueryData(conversationKey("s", "11111111-1111-4111-8111-111111111111"), detail("11111111-1111-4111-8111-111111111111"), { updatedAt: Date.now() - 11_000 });
    await prefetchConversation(qc, c, "11111111-1111-4111-8111-111111111111");
    expect(qc.getQueryData(conversationKey("s", "11111111-1111-4111-8111-111111111111"))).toBeUndefined();
    await prefetchConversation(qc, c, "11111111-1111-4111-8111-111111111111");
    expect(read).toHaveBeenCalledTimes(1);
  });

  it("a transient warm failure keeps the entry and is re-asked", async () => {
    read.mockReset().mockRejectedValue(new TransportError("network", "/v1/conversations/11111111-1111-4111-8111-111111111111", "unreachable"));
    const qc = makeQueryClient();
    qc.setQueryData(conversationKey("s", "11111111-1111-4111-8111-111111111111"), detail("11111111-1111-4111-8111-111111111111"), { updatedAt: Date.now() - 11_000 });
    await prefetchConversation(qc, c, "11111111-1111-4111-8111-111111111111");
    expect(qc.getQueryData(conversationKey("s", "11111111-1111-4111-8111-111111111111"))).toEqual(detail("11111111-1111-4111-8111-111111111111"));
    await prefetchConversation(qc, c, "11111111-1111-4111-8111-111111111111");
    expect(read).toHaveBeenCalledTimes(2);
  });

  it("re-reads an entry past its freshness window", async () => {
    read.mockReset().mockResolvedValue(detail("11111111-1111-4111-8111-111111111111"));
    const qc = makeQueryClient();
    qc.setQueryData(conversationKey("s", "11111111-1111-4111-8111-111111111111"), detail("11111111-1111-4111-8111-111111111111"), { updatedAt: Date.now() - 11_000 });
    prefetchConversation(qc, c, "11111111-1111-4111-8111-111111111111");
    await vi.waitFor(() => expect(read).toHaveBeenCalledTimes(1));
  });

  // Home re-reads all three on mount, so a warm read would be a second one.
  it("warms nothing for Home", () => {
    const qc = makeQueryClient();
    const spy = vi.spyOn(qc, "prefetchQuery");
    prefetchDestination(qc, "/home", { ...ctx("s"), authReady: true, graphIds: ["g1"] });
    expect(spy).not.toHaveBeenCalled();
  });

  it("does nothing before auth is ready", () => {
    read.mockReset();
    const qc = makeQueryClient();
    prefetchConversation(qc, { ...c, authReady: false }, "11111111-1111-4111-8111-111111111111");
    expect(read).not.toHaveBeenCalled();
    expect(qc.getQueryState(conversationKey("s", "11111111-1111-4111-8111-111111111111"))).toBeUndefined();
  });
});
