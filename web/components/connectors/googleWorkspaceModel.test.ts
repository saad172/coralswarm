import { describe, expect, it } from "vitest";
import type { OAuthConnection } from "@/lib/api";
import {
  parseWorkspaceResult,
  shouldShowWorkspaceButton,
  surfaceStatuses,
  workspaceConnectError,
  workspaceResultLines,
  type WorkspaceSurfaceFlags,
} from "./googleWorkspaceModel";

function conn(provider: string, scope: string): OAuthConnection {
  return {
    provider,
    provider_workspace_id: null,
    scope,
    expires_at: null,
    connected_at: "2026-10-01T00:00:00Z",
    missing_scopes: [],
    scopes_unknown: false,
  };
}
const CAL = conn("google", "https://www.googleapis.com/auth/calendar.readonly");
const GMAIL = conn("google_gmail", "https://www.googleapis.com/auth/gmail.readonly");
const CHAT = conn("google_chat", "https://www.googleapis.com/auth/chat.spaces.readonly");
const MEET = conn("google_meet", "https://www.googleapis.com/auth/meetings.space.readonly");

const ALL_ON: WorkspaceSurfaceFlags = [
  { surface: "calendar", enabled: true },
  { surface: "gmail", enabled: true },
  { surface: "chat", enabled: true },
  { surface: "meet", enabled: true },
];

describe("shouldShowWorkspaceButton", () => {
  it("shows when nothing is connected and surfaces are enabled", () => {
    expect(shouldShowWorkspaceButton(surfaceStatuses([], ALL_ON))).toBe(true);
  });

  it("shows while any enabled surface is still unconnected", () => {
    expect(shouldShowWorkspaceButton(surfaceStatuses([CAL, GMAIL, CHAT], ALL_ON))).toBe(true);
  });

  it("hides once every enabled surface is connected", () => {
    expect(shouldShowWorkspaceButton(surfaceStatuses([CAL, GMAIL, CHAT, MEET], ALL_ON))).toBe(false);
  });

  it("ignores surfaces whose flag is off: only the enabled ones count", () => {
    const flags: WorkspaceSurfaceFlags = [
      { surface: "calendar", enabled: true },
      { surface: "gmail", enabled: true },
      { surface: "chat", enabled: false },
      { surface: "meet", enabled: false },
    ];
    expect(shouldShowWorkspaceButton(surfaceStatuses([CAL], flags))).toBe(true);
    expect(shouldShowWorkspaceButton(surfaceStatuses([CAL, GMAIL], flags))).toBe(false);
  });

  it("hides when no surface is enabled, or the flags are unknown", () => {
    const none = ALL_ON.map((f) => ({ ...f, enabled: false }));
    expect(shouldShowWorkspaceButton(surfaceStatuses([], none))).toBe(false);
    expect(shouldShowWorkspaceButton(surfaceStatuses([], null))).toBe(false);
  });

  it("does not count a row from the outgoing account after an account switch", () => {
    const statuses = surfaceStatuses([CAL, GMAIL, CHAT, MEET], ALL_ON, "old-account", "new-account");
    expect(statuses.every((s) => !s.connected)).toBe(true);
    expect(shouldShowWorkspaceButton(statuses)).toBe(true);
  });

  it("does not count a stale grant that lacks the surface's own scope", () => {
    const staleGmail = conn("google_gmail", "https://www.googleapis.com/auth/calendar.readonly");
    expect(surfaceStatuses([staleGmail], ALL_ON).find((s) => s.surface === "gmail")?.connected).toBe(false);
  });
});

describe("parseWorkspaceResult", () => {
  const params = (q: string) => new URLSearchParams(q);

  it("is null on any other page load", () => {
    expect(parseWorkspaceResult(params(""))).toBeNull();
    expect(parseWorkspaceResult(params("connected=google"))).toBeNull();
  });

  it("reads connected, skipped (with reason) and failed surfaces", () => {
    const r = parseWorkspaceResult(
      params("google_workspace=1&gws_connected=calendar,gmail&gws_skipped=chat:unticked,meet:disabled&gws_failed=gmail"),
    );
    expect(r).toEqual({
      connected: ["calendar", "gmail"],
      skipped: [
        { surface: "chat", reason: "unticked" },
        { surface: "meet", reason: "disabled" },
      ],
      failed: ["gmail"],
    });
  });

  it("drops unknown surfaces and reasons instead of guessing", () => {
    const r = parseWorkspaceResult(
      params("google_workspace=1&gws_connected=drive,calendar&gws_skipped=chat:nope,bogus:unticked,meet:unticked"),
    );
    expect(r).toEqual({
      connected: ["calendar"],
      skipped: [{ surface: "meet", reason: "unticked" }],
      failed: [],
    });
  });
});

describe("workspaceResultLines", () => {
  it("names connected surfaces and separates unticked from not-available", () => {
    const lines = workspaceResultLines({
      connected: ["calendar", "gmail"],
      skipped: [
        { surface: "chat", reason: "unticked" },
        { surface: "meet", reason: "disabled" },
      ],
      failed: [],
    });
    expect(lines.map((l) => l.text)).toEqual([
      "Connected: Google Calendar, Gmail.",
      "Skipped, not selected on Google's screen: Google Chat. Use that row's Connect to add it later.",
      "Not available for this workspace: Google Meet.",
    ]);
  });

  it("says so plainly when nothing was connected", () => {
    const lines = workspaceResultLines({
      connected: [],
      skipped: [{ surface: "calendar", reason: "unticked" }],
      failed: [],
    });
    expect(lines[0]).toEqual({ tone: "muted", text: "Nothing was connected." });
  });

  it("reports a failed surface", () => {
    const lines = workspaceResultLines({ connected: ["calendar"], skipped: [], failed: ["chat"] });
    expect(lines.some((l) => l.tone === "bad" && l.text.includes("Google Chat"))).toBe(true);
  });
});

describe("workspaceConnectError", () => {
  it("explains a flag refusal and passes other messages through", () => {
    expect(workspaceConnectError(403, "x")).toMatch(/isn't enabled for this workspace/);
    expect(workspaceConnectError(500, "boom")).toBe("boom");
    expect(workspaceConnectError(null, null)).toMatch(/Couldn't start/);
  });
});
