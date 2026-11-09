// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { QueryClientProvider, type QueryClient } from "@tanstack/react-query";
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { createElement, type ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { makeQueryClient } from "@/lib/queryClient";
import type { Graph } from "@/lib/api";
import type { Activity, ActivityDetail } from "@/lib/journalActivitiesApi";

/* The activities path of Today, page level: the real page, components and
   query layer over a faked wire (`guardedFetch`). Covers the timeline with its
   quiet gaps, the workstream panel with and without a gist, the People / Topics
   filter, Cards / Compact persistence, the detail panel's keyboard stepping and
   the Mine / Team toggle's visibility rules. */

const app = vi.hoisted(() => ({
  search: "d=2026-10-29",
  activitiesEnabled: true, gistEnabled: true,
  graphs: [] as unknown[],
  currentGraph: null as unknown,
  push: vi.fn(),
}));
vi.mock("@/lib/providers", () => ({
  useApp: () => ({
    authReady: true, activitiesEnabled: app.activitiesEnabled, gistEnabled: app.gistEnabled,
    getAuthToken: () => "tok",
    queryScope: "user-a::personal",
    userId: "user_clerk_a",
    internalUserId: "u-me",
    graphs: app.graphs,
    currentGraph: app.currentGraph,
  }),
}));
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: app.push }),
  useSearchParams: () => new URLSearchParams(app.search),
}));
vi.mock("next/link", () => ({
  default: ({ children, href }: { children: ReactNode; href: string }) => createElement("a", { href }, children),
}));

const net = vi.hoisted(() => ({
  handler: (_url: URL): Response => new Response("{}"),
  calls: [] as string[],
}));
vi.mock("@/lib/transport", async (orig) => ({
  ...(await orig<typeof import("@/lib/transport")>()),
  guardedFetch: (path: string) => {
    net.calls.push(path);
    return Promise.resolve(net.handler(new URL(path, "http://wire.test")));
  },
}));

import TodayPage from "@/app/(app)/today/page";

// America/Chicago (vitest.config.ts) is UTC-5 on 29 Oct 2026.
const iso = (hhmmLocal: string) => {
  const [h, m] = hhmmLocal.split(":").map(Number);
  return new Date(Date.UTC(2026, 9, 29, h + 5, m)).toISOString();
};
const owner = { user_id: "u-me", name: "Dana Ross" };
const base = { owner, graph: { id: "g-priv", name: "Private" }, outcome: null, decision_count: 0, source_count: 2, people_count: 0 };

const meeting: Activity = {
  ...base,
  key: "k-meeting",
  kind: "meeting",
  kind_label: "meeting",
  start: iso("09:10"),
  end: iso("10:00"),
  title: "Acme pricing review",
  outcome: "Three usage tiers.",
  people: ["Dana Ross", "Priya Nair"],
  people_count: 2,
  topics: [{ node_id: "t-rate", title: "Rate limiter" }],
  decision_count: 2,
};
const agent: Activity = {
  ...base,
  key: "k-agent",
  kind: "agent",
  kind_label: "agent session",
  start: iso("11:30"),
  end: iso("12:30"),
  title: "Refactor the limiter",
  people: ["Priya Nair"],
  people_count: 1,
  topics: [{ node_id: "t-rate", title: "Rate limiter" }],
};
const slack: Activity = {
  ...base,
  key: "k-slack",
  kind: "slack",
  kind_label: "slack thread",
  owner: { user_id: "u-tom", name: "Tom Becker" },
  start: iso("14:00"),
  end: iso("14:00"),
  title: "#billing",
  people: ["Tom Becker"],
  people_count: 1,
  topics: [{ node_id: "t-bill", title: "Billing" }],
};
const ALL = [meeting, agent, slack];

const stripDays = Array.from({ length: 14 }, (_, i) => ({
  day: new Date(Date.UTC(2026, 9, 17 + i)).toISOString().slice(0, 10),
  count: i === 13 ? 3 : 0,
}));
const detailFor = (a: Activity): ActivityDetail => ({
  ...a,
  decisions: a === meeting ? ["Ship tier B", "Soft cap for Acme"] : [],
  sources: [{ kind: a.kind_label, quote: "We settled on three tiers", meta_label: "MEETING · 09:52", source_item_id: null, atom_id: null, occurred_at: null }],
  reefs: a.topics,
});
const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status });

interface Wire {
  activities?: Activity[];
  activitiesStatus?: number;
  teamStatus?: number;
  gist?: { done: string[]; decided: string[]; open: string[] } | null;
  gistStatus?: number;
}
function serve(w: Wire = {}) {
  net.handler = (url) => {
    const p = url.pathname;
    if (p === "/v1/journal/activities" || p === "/v1/journal/strip") {
      if (w.activitiesStatus) return json({ error: "not found" }, w.activitiesStatus);
      if (url.searchParams.get("who") === "team" && w.teamStatus) return json({ error: "not found" }, w.teamStatus);
      return json({ model: p.endsWith("/strip") ? "journal_strip" : "journal_activities", version: 1, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null, payload: {
        activities: w.activities ?? ALL,
        day_activity: stripDays,
        truncated: false,
        window: { from: "", to: "", tz: "America/Chicago", who: url.searchParams.get("who") },
      } });
    }
    if (p.startsWith("/v1/journal/activities/")) {
      const key = decodeURIComponent(p.slice("/v1/journal/activities/".length));
      const a = ALL.find((x) => x.key === key);
      return a ? json(detailFor(a)) : json({ error: "not found" }, 404);
    }
    if (p === "/v1/journal/gist") {
      if (w.gistStatus) return json({ error: "not found" }, w.gistStatus);
      return json({ model: "journal_gist", version: 1, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null, payload: {
        period_start: "", period_end: "", open: false, who: "mine", granularity: "day", tz: "UTC",
        gist: w.gist === undefined ? { done: ["Shipped the limiter"], decided: ["Tier B"], open: ["Acme soft cap"] } : w.gist,
        compiled_at: null, span: null,
      } });
    }
    return json({}, 500);
  };
}

const graph = (id: string, kind: Graph["kind"]) => ({ id, name: id, kind }) as unknown as Graph;

let client: QueryClient;
function mount() {
  const wrapper = ({ children }: { children: ReactNode }) => createElement(QueryClientProvider, { client }, children);
  return render(createElement(TodayPage), { wrapper });
}

beforeEach(() => {
  app.search = "d=2026-10-29";
  app.graphs = [graph("g-priv", "personal")];
  app.currentGraph = null;
  app.activitiesEnabled = true; app.gistEnabled = true;
  app.push.mockReset();
  net.calls = [];
  window.localStorage.clear();
  client = makeQueryClient();
  const slot = document.createElement("span");
  slot.id = "app-header-meta";
  document.body.appendChild(slot);
  serve();
});
afterEach(() => {
  cleanup();
  client.clear();
  document.getElementById("app-header-meta")?.remove();
});

/** The workstream's People / Topics rail (the detail panel has a People section too). */
const rail = (name: "People" | "Topics") =>
  within(screen.getByRole("complementary", { name: "Workstream" })).getByRole("region", { name });
const cards = () => screen.getAllByRole("button", { name: /Acme pricing review|Refactor the limiter|#billing/ });

describe("Today, day view", () => {
  it("shows the title, meta, timeline with quiet gaps, stats and gist", async () => {
    mount();
    expect(await screen.findByText("Acme pricing review")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Thursday 29 October" })).toBeInTheDocument();
    expect(screen.getByText("3 activities · 09:10–14:00")).toBeInTheDocument();
    expect(screen.getByText("3 events")).toBeInTheDocument();
    // Cards order, quiet rows between them (10:00 to 11:30, 12:30 to 14:00).
    expect(cards().map((c) => c.textContent)).toEqual([
      expect.stringContaining("Acme pricing review"),
      expect.stringContaining("Refactor the limiter"),
      expect.stringContaining("#billing"),
    ]);
    expect(screen.getAllByText(/^quiet\s·\s1h 30m$/)).toHaveLength(2);
    // Workstream: gist rows, stat tiles, chart.
    await screen.findByText("Shipped the limiter");
    for (const label of ["DONE", "DECIDED", "OPEN"]) expect(screen.getByText(label)).toBeInTheDocument();
    const ws = screen.getByRole("complementary", { name: "Workstream" });
    expect(within(ws).getByText("your workstream")).toBeInTheDocument();
    expect(within(ws).getByText("09:10 – 14:00")).toBeInTheDocument();
    expect(within(ws).getByRole("button", { name: "Ask about this day" })).toBeInTheDocument();
    expect(within(ws).getByText("activities").previousSibling).toHaveTextContent("3");
    expect(within(ws).getByText("sessions").previousSibling).toHaveTextContent("1");
    expect(within(ws).getByText("decisions").previousSibling).toHaveTextContent("2");
    expect(within(ws).getByRole("img", { name: "time by hour" })).toBeInTheDocument();
    // No owner on Mine cards.
    expect(within(screen.getByRole("button", { name: /#billing/ })).queryByText("Tom")).not.toBeInTheDocument();
  });

  it("hides the gist rows but keeps stats and charts when the gist 404s", async () => {
    serve({ gistStatus: 404 });
    mount();
    await screen.findByText("Acme pricing review");
    await waitFor(() => expect(net.calls.some((c) => c.startsWith("/v1/journal/gist"))).toBe(true));
    await act(async () => {});
    expect(screen.queryByText("DONE")).not.toBeInTheDocument();
    const ws = screen.getByRole("complementary", { name: "Workstream" });
    expect(within(ws).getByText("meetings")).toBeInTheDocument();
    expect(within(ws).getByRole("img", { name: "time by hour" })).toBeInTheDocument();
  });

  it("hides the gist rows when the period has no gist yet (null)", async () => {
    serve({ gist: null });
    mount();
    await screen.findByText("Acme pricing review");
    await act(async () => {});
    expect(screen.queryByText("DECIDED")).not.toBeInTheDocument();
  });

  it("shows the empty state copy", async () => {
    serve({ activities: [] });
    mount();
    expect(await screen.findByText("Nothing captured this day")).toBeInTheDocument();
    expect(screen.getByText("No meetings, sessions or threads you were part of.")).toBeInTheDocument();
  });

  it("leaves me out of People and filters the timeline from a chip, then clears it", async () => {
    mount();
    await screen.findByText("Acme pricing review");
    const people = rail("People");
    expect(within(people).queryByText("Dana")).not.toBeInTheDocument();
    const priya = within(people).getByRole("button", { name: /Priya/ });
    expect(priya).toHaveAttribute("aria-pressed", "false");

    fireEvent.click(priya);
    expect(screen.getByRole("button", { name: "with Priya Nair" })).toBeInTheDocument();
    expect(screen.getByText("2 events")).toBeInTheDocument();
    expect(screen.queryByText("#billing")).not.toBeInTheDocument();
    expect(rail("People").querySelector('[aria-pressed="true"]')).not.toBeNull();

    // The same chip again clears.
    fireEvent.click(within(rail("People")).getByRole("button", { name: /Priya/ }));
    expect(screen.getByText("3 events")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "with Priya Nair" })).not.toBeInTheDocument();

    // A topic filters too, and the pill clears it.
    fireEvent.click(within(rail("Topics")).getByRole("button", { name: /Billing/ }));
    expect(screen.getByText("1 event")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "topic: Billing" }));
    expect(screen.getByText("3 events")).toBeInTheDocument();
  });

  it("persists Cards / Compact per user and drops the quiet rows in Compact", async () => {
    const first = mount();
    await screen.findByText("Acme pricing review");
    expect(screen.getByRole("tab", { name: "Cards" })).toHaveAttribute("aria-selected", "true");

    fireEvent.click(screen.getByRole("tab", { name: "Compact" }));
    expect(screen.getByRole("tab", { name: "Compact" })).toHaveAttribute("aria-selected", "true");
    expect(screen.queryByText(/^quiet/)).not.toBeInTheDocument();
    expect(window.localStorage.getItem("cs.today.density:user_clerk_a")).toBe("compact");

    first.unmount();
    mount();
    await screen.findByText("Acme pricing review");
    await waitFor(() => expect(screen.getByRole("tab", { name: "Compact" })).toHaveAttribute("aria-selected", "true"));
  });

  it("switches range through the URL, keeping the day", async () => {
    mount();
    await screen.findByText("Acme pricing review");
    fireEvent.click(screen.getByRole("tab", { name: "Week" }));
    expect(app.push).toHaveBeenCalledWith("/today?d=2026-10-29&z=week");
    fireEvent.click(screen.getByRole("tab", { name: "Month" }));
    expect(app.push).toHaveBeenLastCalledWith("/today?d=2026-10-29&z=month");
  });

  it("picks a day from the strip", async () => {
    mount();
    await screen.findByText("Acme pricing review");
    const strip = screen.getByRole("group", { name: "Last 14 days" });
    expect(within(strip).getAllByRole("button")).toHaveLength(14);
    fireEvent.click(within(strip).getByRole("button", { name: "wed 28 oct" }));
    expect(app.push).toHaveBeenCalledWith("/today?d=2026-10-28");
  });
});

describe("Today, detail panel", () => {
  it("opens from a card, steps with wrap, and closes with Esc", async () => {
    mount();
    fireEvent.click(await screen.findByRole("button", { name: /Acme pricing review/ }));
    const panel = await screen.findByRole("dialog", { name: /Acme pricing review/ });
    expect(within(panel).getByText(/1 of 3/)).toBeInTheDocument();
    // The detail read fills DECIDED and CAME FROM.
    expect(await within(panel).findByText("Ship tier B")).toBeInTheDocument();
    expect(within(panel).getByText("We settled on three tiers")).toBeInTheDocument();
    expect(within(panel).getByRole("link", { name: "Rate limiter" })).toHaveAttribute("href", expect.stringContaining("/topics?id=t-rate"));

    fireEvent.keyDown(window, { key: "ArrowDown" });
    expect(await screen.findByRole("dialog", { name: /Refactor the limiter/ })).toBeInTheDocument();
    fireEvent.keyDown(window, { key: "ArrowRight" });
    expect(await screen.findByRole("dialog", { name: /#billing/ })).toBeInTheDocument();
    // Wraps forward past the end ...
    fireEvent.keyDown(window, { key: "ArrowDown" });
    expect(await screen.findByRole("dialog", { name: /Acme pricing review/ })).toBeInTheDocument();
    // ... and backward past the start.
    fireEvent.keyDown(window, { key: "ArrowUp" });
    expect(await screen.findByRole("dialog", { name: /#billing/ })).toBeInTheDocument();
    fireEvent.keyDown(window, { key: "ArrowLeft" });
    expect(await screen.findByRole("dialog", { name: /Refactor the limiter/ })).toBeInTheDocument();

    fireEvent.keyDown(window, { key: "Escape" });
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
  });

  it("steps through the filtered list only", async () => {
    mount();
    await screen.findByText("Acme pricing review");
    fireEvent.click(within(rail("People")).getByRole("button", { name: /Priya/ }));
    fireEvent.click(screen.getByRole("button", { name: /Acme pricing review/ }));
    const panel = await screen.findByRole("dialog", { name: /Acme pricing review/ });
    expect(within(panel).getByText(/1 of 2/)).toBeInTheDocument();
    fireEvent.keyDown(window, { key: "ArrowDown" });
    expect(await screen.findByRole("dialog", { name: /Refactor the limiter/ })).toBeInTheDocument();
    fireEvent.keyDown(window, { key: "ArrowDown" });
    expect(await screen.findByRole("dialog", { name: /Acme pricing review/ })).toBeInTheDocument();
  });

  it("closes the panel when a chip filters its card away", async () => {
    mount();
    fireEvent.click(await screen.findByRole("button", { name: /#billing/ }));
    await screen.findByRole("dialog", { name: /#billing/ });
    fireEvent.click(within(rail("People")).getByRole("button", { name: /Priya/ }));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });
});

describe("Today, Mine / Team", () => {
  it("hides the toggle when the viewer has no team or org ocean", async () => {
    mount();
    await screen.findByText("Acme pricing review");
    expect(screen.queryByRole("tab", { name: "Team" })).not.toBeInTheDocument();
    expect(screen.queryByRole("tab", { name: "Mine" })).not.toBeInTheDocument();
  });

  it("shows the toggle for a team ocean and pushes who=team", async () => {
    app.graphs = [graph("g-priv", "personal"), graph("g-team", "team")];
    mount();
    await screen.findByText("Acme pricing review");
    expect(screen.getByRole("tab", { name: "Mine" })).toHaveAttribute("aria-selected", "true");
    fireEvent.click(screen.getByRole("tab", { name: "Team" }));
    expect(app.push).toHaveBeenCalledWith("/today?d=2026-10-29&who=team");
  });

  it("reads Team from the switcher's workspace, shows owners who are not me, and keeps me in People", async () => {
    app.graphs = [graph("g-priv", "personal"), graph("g-team", "team"), graph("g-org", "org")];
    app.currentGraph = graph("g-org", "org");
    app.search = "d=2026-10-29&who=team";
    mount();
    await screen.findByText("Acme pricing review");
    const listCall = net.calls.find((c) => c.startsWith("/v1/journal/activities?") && c.includes("who=team"));
    expect(listCall).toContain("graph_id=g-org");
    expect(screen.getByText("team · 3 activities · 09:10–14:00")).toBeInTheDocument();
    expect(screen.getByText("team workstream")).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Team" })).toHaveAttribute("aria-selected", "true");
    // Tom's card shows its owner; my cards do not.
    const billing = screen.getByRole("button", { name: /#billing/ });
    expect(within(billing).getByText("Tom")).toBeInTheDocument();
    expect(within(screen.getByRole("button", { name: /Acme pricing review/ })).queryByText("Dana")).not.toBeInTheDocument();
    expect(within(rail("People")).getByRole("button", { name: /Dana/ })).toBeInTheDocument();
  });

  it("falls back to the first team ocean when the switcher is on a private one", async () => {
    app.graphs = [graph("g-priv", "personal"), graph("g-team", "team")];
    app.currentGraph = graph("g-priv", "personal");
    app.search = "d=2026-10-29&who=team";
    mount();
    await screen.findByText("Acme pricing review");
    expect(net.calls.find((c) => c.includes("who=team"))).toContain("graph_id=g-team");
  });

  it.each([403, 404])("keeps Team refusal %s explicit until Mine is selected", async (status) => {
    serve({ teamStatus: status });
    app.graphs = [graph("g-priv", "personal"), graph("g-team", "team")];
    app.search = "d=2026-10-29&who=team";
    const view = mount();
    await screen.findByRole("button", { name: "Retry Today" });
    expect(screen.queryByText("Acme pricing review")).not.toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Team" })).toHaveAttribute("aria-selected", "true");
    expect(net.calls.filter((call) => call.startsWith("/v1/journal/activities?") && call.includes("who=mine"))).toHaveLength(0);
    fireEvent.click(screen.getByRole("tab", { name: "Mine" }));
    expect(app.push).toHaveBeenCalledWith("/today?d=2026-10-29");
    app.search = "d=2026-10-29";
    view.rerender(createElement(TodayPage));
    expect(await screen.findByText("Acme pricing review")).toBeInTheDocument();
  });

  it("ignores who=team when there is no workspace", async () => {
    app.search = "d=2026-10-29&who=team";
    mount();
    await screen.findByText("Acme pricing review");
    expect(net.calls.some((c) => c.includes("who=team"))).toBe(false);
    expect(screen.getByText("your workstream")).toBeInTheDocument();
  });
});

describe("Today, week and month", () => {
  it("groups the week by day and the month by week, always compact", async () => {
    app.search = "d=2026-10-29&z=week";
    const week = mount();
    await screen.findByText("Acme pricing review");
    expect(screen.getByRole("heading", { name: "Week of 26 October – 1 November" })).toBeInTheDocument();
    expect(screen.getByText("thu 29 oct")).toBeInTheDocument();
    expect(screen.getByRole("img", { name: "time by day" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Compact" })).toBeInTheDocument();
    week.unmount();

    app.search = "d=2026-10-29&z=month";
    mount();
    await screen.findByText("Acme pricing review");
    expect(screen.getByRole("heading", { name: "October 2026" })).toBeInTheDocument();
    expect(screen.getByText("week of 26 oct")).toBeInTheDocument();
    expect(screen.queryByRole("tab", { name: "Cards" })).not.toBeInTheDocument();
    expect(screen.getByRole("img", { name: "time by day · last 14 days" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Ask about this month" })).toBeInTheDocument();
  });
});

it("activities-on and gist-off selects activities and strip in parallel without a gist probe", async () => {
  app.gistEnabled = false;
  mount();
  await screen.findByText("Acme pricing review");
  expect(net.calls.some((path) => path.startsWith("/v1/journal/activities?"))).toBe(true);
  expect(net.calls.some((path) => path.startsWith("/v1/journal/strip?"))).toBe(true);
  expect(net.calls.some((path) => path.startsWith("/v1/journal/gist"))).toBe(false);
});


it("keeps a pending recap distinct from a computed empty recap", async () => {
  const original = net.handler;
  net.handler = (url) => url.pathname === "/v1/journal/gist" ? json({ model: "journal_gist", version: 1,
    computed_at: null, stale: true, refresh_eta: "2026-10-31T00:00:02Z", payload: { gist: null } }) : original(url);
  mount();
  expect(await screen.findByText("Acme pricing review")).toBeInTheDocument();
  expect(screen.getByText(/reading the recap/)).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Refresh Today" })).toBeDisabled();
  expect(screen.getByText("Refreshing…")).toBeInTheDocument();
});
