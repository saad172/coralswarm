import { describe, expect, it } from "vitest";
import {
  abandonedSignIn, canDisconnect, captureSummary, keepWaiting, kindList, kindPlural, lastSignInExpired, limitEntries,
  listedConnections, liveConnections, needsReconnect, parseToolkit, pendingConnections, toolkitFallbackLabel, toolkitLink, type ComposioConnection, type ComposioStatus,
} from "./composioModel";

const now = Date.parse("2026-10-13T12:00:00Z");
const connection: ComposioConnection = {
  id: "c1", status: "active", provider_namespace: "linear:workspace:w", self_check: "passed", created_at: "2026-10-13T10:00:00Z", message: null,
  capture: {
    id: "cap", graph_id: "g", connection_id: "c1", service: "linear", provider_namespace: "linear:workspace:w", state: "active",
    captured_items: 3, watermark: "2026-10-13T11:50:00Z", pass_started_at: null, next_poll_at: "2026-10-13T12:15:00Z",
    last_change_at: null, last_capture_at: null, retry_at: null, error_code: null, created_at: "2026-10-13T10:00:00Z",
  },
};
const status = (connections: ComposioConnection[]): ComposioStatus => ({
  service: "linear", label: "Linear", toolkit_version: "v", kinds: ["issue", "comment", "project"], configured: true,
  polling_enabled: true, beta: true, connections, limits: {},
});

describe("connect-and-done status copy", () => {
  it("describes active capture from the moment of connect, with count and next check", () => {
    const text = captureSummary(connection, now);
    expect(text).toContain("Capturing everything that changes since");
    expect(text).toContain("3 items captured");
    expect(text).toContain("Next check in 15 min");
  });
  it("shows the backend's plain message for every paused state", () => {
    const message = "This tool isn't working with CoralSwarm yet.";
    expect(captureSummary({ ...connection, status: "needs_attention", message }, now)).toBe(message);
  });
  it("tells the member a retry is temporary", () => {
    expect(captureSummary({ ...connection, capture: { ...connection.capture!, state: "retry" } }, now)).toContain("try again");
  });
  it("offers reconnect only after revocation and disconnect until disconnected", () => {
    expect(needsReconnect({ ...connection, status: "reconnect_needed" })).toBe(true);
    expect(needsReconnect(connection)).toBe(false);
    expect(canDisconnect(connection)).toBe(true);
    expect(canDisconnect({ ...connection, status: "disconnected" })).toBe(false);
  });
  it("separates pending and historical connections", () => {
    const s = status([connection, { ...connection, id: "p", status: "pending" }, { ...connection, id: "d", status: "disconnected" }]);
    expect(pendingConnections(s).map(c => c.id)).toEqual(["p"]);
    expect(liveConnections(s).map(c => c.id)).toEqual(["c1", "p"]);
  });
});
describe("waiting for sign-in", () => {
  const apiError = (status: number, code: string | null = null) => Object.assign(new Error("x"), { status, code });
  it("keeps waiting only while Composio waits for sign-in or on a transient failure", () => {
    expect(keepWaiting(apiError(409, "sign_in_pending"))).toBe(true);
    for (const transient of [apiError(0), apiError(429), apiError(502), apiError(503), new TypeError("fetch failed")]) {
      expect(keepWaiting(transient)).toBe(true);
    }
  });
  it("stops on an expired session, a missing or terminal connection, or a rejected grant", () => {
    for (const terminal of [
      apiError(401), apiError(403), apiError(404), apiError(410, "sign_in_expired"),
      // Disconnected or bound to another workspace: asking again cannot finish it.
      apiError(409), apiError(409, "something_else"),
    ]) expect(keepWaiting(terminal)).toBe(false);
  });
  it("hides abandoned sign-ins from the list and offers to connect again", () => {
    const abandoned: ComposioConnection = { ...connection, id: "x", status: "reconnect_needed", provider_namespace: null, capture: null, message: "Signing in didn't finish." };
    const revoked: ComposioConnection = { ...connection, id: "r", status: "reconnect_needed" };
    const s = status([revoked, abandoned]);
    expect(abandonedSignIn(abandoned)).toBe(true);
    expect(abandonedSignIn(revoked)).toBe(false);
    expect(listedConnections(s).map(c => c.id)).toEqual(["r"]);
    expect(lastSignInExpired(s)?.id).toBe("x");
    expect(lastSignInExpired(status([abandoned, connection]))).toBeNull();
    expect(lastSignInExpired(status([]))).toBeNull();
  });
});
describe("recipe-driven labels", () => {
  it("derives labels from the toolkit and recipe, not a per-service switch", () => {
    expect(toolkitFallbackLabel("google_drive")).toBe("Google Drive");
    expect(kindList(["issue", "comment", "project"])).toBe("issues, comments and projects");
    expect(kindList(["issue"])).toBe("issues");
    expect(kindList([])).toBe("items");
  });
  it("lists only textual coverage statements", () => {
    expect(limitEntries({ state_history: "unavailable" })).toEqual([["state history", "unavailable"]]);
  });
});
describe("generic toolkit routing", () => {
  it("accepts only recipe-shaped slugs from the query string", () => {
    expect(parseToolkit("linear")).toBe("linear");
    expect(parseToolkit("google_drive")).toBe("google_drive");
    for (const bad of [null, "", "Linear", "../admin", "jira/connect", "a b", "x".repeat(65)]) expect(parseToolkit(bad)).toBeNull();
  });
  it("links every registered toolkit to the generic page and labels beta toolkits", () => {
    expect(toolkitLink({ toolkit: "linear", label: "Linear", kinds: ["issue"], beta: true })).toEqual({ href: "/connectors/composio?toolkit=linear", text: "Linear capture · Beta" });
    expect(toolkitLink({ toolkit: "jira", label: "Jira", kinds: ["issue"], beta: false }).text).toBe("Jira capture");
  });
  it("pluralizes recipe content kinds for copy", () => {
    expect(kindPlural("issue")).toBe("issues");
    expect(kindPlural("pull_request")).toBe("pull requests");
    expect(kindPlural("story")).toBe("stories");
    expect(kindPlural("inbox")).toBe("inboxes");
  });
});
