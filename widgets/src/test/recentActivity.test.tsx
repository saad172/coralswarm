import { afterEach, describe, it, expect } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/preact";
import { RecentActivityTemplate } from "../templates/RecentActivity";
import { installFakeOpenAi } from "./fakeOpenAi";

import defaultFixture from "../../fixtures/recent_activity.default.json";
import emptyFixture from "../../fixtures/recent_activity.empty.json";

// jsdom's default viewport. The preview count now depends on the host's
// reported width (see `previewCountFor`), and with no MCP host in these
// legacy-bridge tests the width comes from `window.innerWidth` — so every
// test that cares pins it explicitly and this restores the default after.
const JSDOM_WIDTH = 1024;
function setViewportWidth(width: number) {
  Object.defineProperty(window, "innerWidth", { value: width, configurable: true });
}
afterEach(() => setViewportWidth(JSDOM_WIDTH));

function manyEvents(count: number) {
  return Array.from({ length: count }, (_, index) => ({
    ...defaultFixture.toolOutput.events[0],
    id: `event-${index}`,
    payload: { citation_count: index + 1 },
  }));
}

describe("RecentActivityTemplate", () => {
  it.each([
    [390, 3, "compact"],
    [768, 5, "regular"],
    [1280, 8, "wide"],
  ] as const)("previews %d-wide as %d events (%s tier) when the host reports no height budget", async (width, expected, _tier) => {
    setViewportWidth(width);
    installFakeOpenAi({ toolOutput: { ...defaultFixture.toolOutput, events: manyEvents(21) } });
    render(<RecentActivityTemplate />);
    expect(await screen.findByText("recent activity · 21 events")).toBeTruthy();
    expect(screen.getAllByText(/^Query run/)).toHaveLength(expected);
    expect(screen.getByText(`${expected} of 21 events shown`)).toBeTruthy();
  });

  it.each([
    // floor((maxHeight - 120 chrome) / 72 per row), clamped to [3, 10].
    [500, 5],
    [150, 3],
    [2000, 10],
  ] as const)("derives the preview count from a %dpx host height budget", async (maxHeight, expected) => {
    setViewportWidth(768);
    installFakeOpenAi({ maxHeight, toolOutput: { ...defaultFixture.toolOutput, events: manyEvents(21) } });
    render(<RecentActivityTemplate />);
    expect(await screen.findByText("recent activity · 21 events")).toBeTruthy();
    expect(screen.getAllByText(/^Query run/)).toHaveLength(expected);
  });

  it("bounds the event body under a host cap too short for the three-event minimum", async () => {
    setViewportWidth(768);
    installFakeOpenAi({ maxHeight: 179, toolOutput: { ...defaultFixture.toolOutput, events: manyEvents(21) } });
    render(<RecentActivityTemplate />);
    await screen.findByText("recent activity · 21 events");
    const body = document.querySelector("[data-scroll-body]") as HTMLElement;
    expect(body.getAttribute("data-scroll-body")).toBe("bounded");
    // 179 - 120 chrome = 59: a thin scroller, but the card then fits inside
    // the cap so the footer's toggle survives (verified in a real browser).
    expect(body.style.maxHeight).toBe("59px");
    expect(body.style.overflowY).toBe("auto");
    // The footer and its toggle stay OUTSIDE the scroller.
    expect(body.contains(screen.getByRole("button", { name: /Show all/ }))).toBe(false);
  });

  it("never lets the bounded body's own floor push the card past the cap", async () => {
    setViewportWidth(768);
    installFakeOpenAi({ maxHeight: 130, toolOutput: { ...defaultFixture.toolOutput, events: manyEvents(21) } });
    render(<RecentActivityTemplate />);
    await screen.findByText("recent activity · 21 events");
    const body = document.querySelector("[data-scroll-body]") as HTMLElement;
    expect(body.style.maxHeight).toBe("10px");
    expect(screen.getByRole("button", { name: /Show all/ })).toBeTruthy();
  });

  it("bounds the event body to the cap's remaining budget, and not at all without a cap", async () => {
    setViewportWidth(768);
    const output = { ...defaultFixture.toolOutput, events: manyEvents(21) };
    installFakeOpenAi({ maxHeight: 600, toolOutput: output });
    const bounded = render(<RecentActivityTemplate />);
    await screen.findByText("recent activity · 21 events");
    expect((document.querySelector("[data-scroll-body]") as HTMLElement).style.maxHeight).toBe("480px");
    bounded.unmount();

    installFakeOpenAi({ toolOutput: output });
    render(<RecentActivityTemplate />);
    await screen.findByText("recent activity · 21 events");
    const free = document.querySelector("[data-scroll-body]") as HTMLElement;
    expect(free.getAttribute("data-scroll-body")).toBe("unbounded");
    expect(free.style.maxHeight).toBe("");
  });

  it("shows every event in fullscreen, with no toggle", async () => {
    setViewportWidth(390);
    installFakeOpenAi({ displayMode: "fullscreen", maxHeight: 400, toolOutput: { ...defaultFixture.toolOutput, events: manyEvents(21) } });
    render(<RecentActivityTemplate />);
    expect(await screen.findByText("recent activity · 21 events")).toBeTruthy();
    expect(screen.getAllByText(/^Query run/)).toHaveLength(21);
    expect(screen.queryByRole("button", { name: /Show all/ })).toBeNull();
  });

  it("drops the time gutter column and moves the time inline at compact width", async () => {
    setViewportWidth(390);
    installFakeOpenAi({ toolOutput: defaultFixture.toolOutput });
    render(<RecentActivityTemplate />);
    await screen.findByText("Query run · 4 citations.");
    const rows = document.querySelectorAll("[data-time-column]");
    expect(rows.length).toBeGreaterThan(0);
    for (const row of rows) {
      expect(row.getAttribute("data-time-column")).toBe("inline");
      expect((row as HTMLElement).style.gridTemplateColumns).toBe("minmax(0, 1fr)");
    }
  });

  it("keeps the time gutter column at regular width", async () => {
    setViewportWidth(768);
    installFakeOpenAi({ toolOutput: defaultFixture.toolOutput });
    render(<RecentActivityTemplate />);
    await screen.findByText("Query run · 4 citations.");
    const row = document.querySelector("[data-time-column]") as HTMLElement;
    expect(row.getAttribute("data-time-column")).toBe("gutter");
    expect(row.style.gridTemplateColumns).toBe("40px minmax(0, 1fr)");
  });

  it("previews five events at the ChatGPT container width, exposes the rest on demand, and resets for a new result", async () => {
    setViewportWidth(768);
    const events = manyEvents(21);
    const output = { ...defaultFixture.toolOutput, events, has_more: true };
    const openai = installFakeOpenAi({ toolOutput: output });
    render(<RecentActivityTemplate />);
    expect(await screen.findByText("recent activity · 21 events")).toBeTruthy();
    expect(screen.getAllByText(/^Query run/)).toHaveLength(5);
    expect(screen.queryByText("Query run · 21 citations.")).toBeNull();
    expect(screen.getByText("5 of 21 events shown")).toBeTruthy();
    const toggle = screen.getByRole("button", { name: "Show all 21 events" });
    expect(toggle.getAttribute("aria-expanded")).toBe("false");
    fireEvent.click(toggle);
    expect(screen.getAllByText(/^Query run/)).toHaveLength(21);
    expect(toggle.getAttribute("aria-expanded")).toBe("true");
    expect(screen.getByText("21 of 21 events shown")).toBeTruthy();
    expect(screen.getByText("ask for the next page")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Show fewer events" }));
    expect(screen.getAllByText(/^Query run/)).toHaveLength(5);
    fireEvent.click(screen.getByRole("button", { name: "Show all 21 events" }));
    openai.setGlobals({ toolOutput: { ...output, page: 2, events: [...events] } });
    await waitFor(() => expect(screen.getAllByText(/^Query run/)).toHaveLength(5));
  });

  it("groups events by day and renders a badge/body per real event_type", async () => {
    installFakeOpenAi({ toolInput: defaultFixture.toolInput, toolOutput: defaultFixture.toolOutput });
    render(<RecentActivityTemplate />);
    expect(await screen.findByText("Query run · 4 citations.")).toBeTruthy();
    expect(screen.getByText(/14 new corals ingested from slack\./)).toBeTruthy();
    expect(screen.getByText(/3 corals promoted\./)).toBeTruthy();
    expect(screen.getByText(/A coral was marked as superseding another\./)).toBeTruthy();
  });

  it("renders Empty when there are no events", async () => {
    installFakeOpenAi({ toolInput: emptyFixture.toolInput, toolOutput: emptyFixture.toolOutput });
    render(<RecentActivityTemplate />);
    expect(await screen.findByText(/Nothing has happened yet\./)).toBeTruthy();
  });
});
