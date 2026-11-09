import { describe, expect, it, vi } from "vitest";
import { deliverAuthResult, type CallbackDeps, type TeamsSdk } from "./AuthCallback";
import type { OfficeApi } from "@/lib/addin/office";
import { createState } from "@/lib/addin/clerkOAuth";

const ORIGIN = "https://app.coralswarm.example";

function harness(search: string, overrides: Partial<CallbackDeps> = {}) {
  const messageParent = vi.fn();
  const office = { context: { ui: { messageParent } } } as unknown as OfficeApi;
  const notifySuccess = vi.fn();
  const notifyFailure = vi.fn();
  const teams: TeamsSdk = { app: { initialize: vi.fn(async () => {}) }, authentication: { notifySuccess, notifyFailure } };
  const deps: CallbackDeps = { search, origin: ORIGIN, scrub: vi.fn(), loadOffice: async () => office, loadTeams: async () => teams, ...overrides };
  return { deps, messageParent, notifySuccess, notifyFailure, teams };
}

describe("callback page", () => {
  it("hands the code and state to the Outlook pane, targeted at its own origin", async () => {
    const state = createState("outlook");
    const h = harness(`?code=abc&state=${state}`);
    expect(await deliverAuthResult(h.deps)).toBe("sent");
    expect(h.messageParent).toHaveBeenCalledWith(JSON.stringify({ code: "abc", state }), { targetOrigin: ORIGIN });
  });

  it("still delivers when history.replaceState throws (Office.js nulls it)", async () => {
    const state = createState("outlook");
    const h = harness(`?code=abc&state=${state}`, { scrub: () => { throw new TypeError("history.replaceState is not a function"); } });
    expect(await deliverAuthResult(h.deps)).toBe("sent");
    expect(h.messageParent).toHaveBeenCalledTimes(1);
  });

  it("scrubs the query before it answers", async () => {
    const state = createState("outlook");
    const order: string[] = [];
    const h = harness(`?code=abc&state=${state}`, { scrub: () => { order.push("scrub"); } });
    h.messageParent.mockImplementation(() => { order.push("send"); });
    await deliverAuthResult(h.deps);
    expect(order).toEqual(["scrub", "send"]);
  });

  it("retries without options on clients that reject targetOrigin", async () => {
    const state = createState("outlook");
    const h = harness(`?code=abc&state=${state}`);
    h.messageParent.mockImplementationOnce(() => { throw new Error("bad options"); });
    expect(await deliverAuthResult(h.deps)).toBe("sent");
    expect(h.messageParent).toHaveBeenLastCalledWith(JSON.stringify({ code: "abc", state }));
  });

  it("forwards an authorize error so the pane can say the sign-in was cancelled", async () => {
    const state = createState("outlook");
    const h = harness(`?error=access_denied&state=${state}`);
    expect(await deliverAuthResult(h.deps)).toBe("sent");
    expect(JSON.parse(h.messageParent.mock.calls[0][0])).toEqual({ error: "access_denied", state });
  });

  it("answers a Teams dialog through the Teams SDK", async () => {
    const state = createState("teams");
    const h = harness(`?code=tc&state=${state}`);
    expect(await deliverAuthResult(h.deps)).toBe("sent");
    expect(h.teams.app.initialize).toHaveBeenCalled();
    expect(h.notifySuccess).toHaveBeenCalledWith(JSON.stringify({ code: "tc", state }));
    expect(h.messageParent).not.toHaveBeenCalled();
    const denied = harness(`?error=access_denied&state=${state}`);
    await deliverAuthResult(denied.deps);
    expect(denied.notifyFailure).toHaveBeenCalledWith("access_denied");
  });

  it("answers the Meet side panel through window.opener, targeted at its own origin, then closes", async () => {
    const state = createState("meet");
    const postMessage = vi.fn();
    const closeSelf = vi.fn();
    const scrub = vi.fn(() => { throw new TypeError("replaceState unavailable"); });
    const h = harness(`?code=gc&state=${state}`, { opener: () => ({ postMessage }), closeSelf, scrub });
    expect(await deliverAuthResult(h.deps)).toBe("sent");
    expect(postMessage).toHaveBeenCalledWith({ code: "gc", state }, ORIGIN);
    expect(closeSelf).toHaveBeenCalledTimes(1);
    expect(postMessage.mock.invocationCallOrder[0]).toBeLessThan(closeSelf.mock.invocationCallOrder[0]);
    expect(h.messageParent).not.toHaveBeenCalled();
    expect(h.notifySuccess).not.toHaveBeenCalled();

    const denied = vi.fn();
    await deliverAuthResult(harness(`?error=access_denied&state=${state}`, { opener: () => ({ postMessage: denied }), closeSelf: vi.fn() }).deps);
    expect(denied).toHaveBeenCalledWith({ error: "access_denied", state }, ORIGIN);
  });

  it("fails (and posts nothing) for a Meet state with no opener", async () => {
    const state = createState("meet");
    const closeSelf = vi.fn();
    expect(await deliverAuthResult(harness(`?code=gc&state=${state}`, { opener: () => null, closeSelf }).deps)).toBe("failed");
    expect(closeSelf).not.toHaveBeenCalled();
  });

  it("sends nothing for a missing or malformed state, or no code", async () => {
    for (const search of ["", "?code=abc", "?code=abc&state=short", `?state=${createState("outlook")}`, "?code=abc&state=" + "x".repeat(33)]) {
      const h = harness(search);
      expect(await deliverAuthResult(h.deps)).toBe("invalid");
      expect(h.messageParent).not.toHaveBeenCalled();
      expect(h.notifySuccess).not.toHaveBeenCalled();
    }
  });

  it("reports failure when the host cannot be reached", async () => {
    const state = createState("outlook");
    expect(await deliverAuthResult(harness(`?code=abc&state=${state}`, { loadOffice: async () => { throw new Error("Not running in an Office host"); } }).deps)).toBe("failed");
  });
});
