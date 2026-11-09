import { describe, expect, it } from "vitest";
import { ApiError, type ConversationSummary, type OAuthConnection, type OrgDirectoryMember } from "@/lib/api";
import {
  connectorsOffInOrg,
  deriveFirstRunChecklist,
  FIRST_RUN_SNOOZE_MS,
  ownConversationCount,
  teammateCount,
  type FirstRunInput,
} from "@/lib/firstRunChecklist";

const NOW = Date.parse("2026-10-20T12:00:00Z");

function conn(provider: string, over: Partial<OAuthConnection> = {}): OAuthConnection {
  return {
    provider,
    provider_workspace_id: null,
    scope: "",
    expires_at: null,
    connected_at: "2026-10-01T00:00:00Z",
    missing_scopes: [],
    scopes_unknown: false,
    credential_invalid_at: null,
    credential_invalid_reason: null,
    mine: true,
    ...over,
  };
}

function input(over: Partial<FirstRunInput> = {}): FirstRunInput {
  return { hasOrg: true, connections: [], teammates: 0, conversationCount: 0, snoozedUntil: null, now: NOW, ...over };
}

describe("deriveFirstRunChecklist", () => {
  it("shows every step as to-do, in order, for a brand-new org admin", () => {
    const c = deriveFirstRunChecklist(input());
    expect(c.visible).toBe(true);
    expect(c.items.map((i) => [i.id, i.state])).toEqual([
      ["connect", "todo"],
      ["invite", "todo"],
      ["ask", "todo"],
    ]);
  });

  it("marks inviting done once anyone else holds a place, invited or signed in", () => {
    const c = deriveFirstRunChecklist(input({ teammates: 1 }));
    expect(c.items[1]).toMatchObject({ id: "invite", state: "done", teammates: 1 });
  });

  it("leaves inviting out for a member who cannot invite", () => {
    const c = deriveFirstRunChecklist(
      input({ teammates: "unavailable", connections: [conn("google_gmail")], conversationCount: 1 }),
    );
    expect(c.items[1].state).toBe("unavailable");
    expect(c.complete).toBe(true);
    expect(c.visible).toBe(false);
  });

  it("does not render while the teammate count is still loading", () => {
    expect(deriveFirstRunChecklist(input({ teammates: null })).visible).toBe(false);
  });

  it("never shows outside a real organization", () => {
    expect(deriveFirstRunChecklist(input({ hasOrg: false })).visible).toBe(false);
  });

  it("does not render while either input is still loading", () => {
    expect(deriveFirstRunChecklist(input({ connections: null })).visible).toBe(false);
    expect(deriveFirstRunChecklist(input({ conversationCount: null })).visible).toBe(false);
  });

  it("hides once every step is done", () => {
    const c = deriveFirstRunChecklist(
      input({ connections: [conn("google_gmail")], teammates: 2, conversationCount: 2 }),
    );
    expect(c.complete).toBe(true);
    expect(c.visible).toBe(false);
  });

  it("counts only the member's own grant, never an account-wide row (D21)", () => {
    const c = deriveFirstRunChecklist(input({ connections: [conn("slack", { mine: false })], conversationCount: 1 }));
    expect(c.items[0]).toMatchObject({ id: "connect", state: "todo", connected: [], shared: ["Slack"] });
    expect(c.visible).toBe(true);
  });

  it("treats a missing `mine` (older backend) as not the member's own", () => {
    const c = deriveFirstRunChecklist(input({ connections: [conn("slack_user", { mine: undefined })] }));
    expect(c.items[0].state).toBe("todo");
  });

  it("does not count a revoked credential as connected", () => {
    const c = deriveFirstRunChecklist(
      input({ connections: [conn("google_gmail", { credential_invalid_at: "2026-10-10T00:00:00Z" })] }),
    );
    expect(c.items[0]).toMatchObject({ state: "todo", needsReconnect: ["Gmail"] });
  });

  it("names each connected source once", () => {
    const c = deriveFirstRunChecklist(
      input({ connections: [conn("slack_user"), conn("slack_user"), conn("google_gmail")] }),
    );
    expect(c.items[0]).toMatchObject({ state: "done", connected: ["Slack", "Gmail"] });
  });

  it("marks the ask step done from the conversation count", () => {
    const c = deriveFirstRunChecklist(input({ conversationCount: 1 }));
    expect(c.items[2].state).toBe("done");
    expect(c.visible).toBe(true);
  });

  it("reports an unreadable input as unknown rather than done", () => {
    const c = deriveFirstRunChecklist(input({ connections: "error", conversationCount: 0 }));
    expect(c.items[0].state).toBe("unknown");
    expect(c.complete).toBe(false);
    expect(c.visible).toBe(true);
  });

  it("stays hidden when nothing is known to be left to do", () => {
    // Done + unknown: we cannot claim incompleteness, so no checklist.
    const c = deriveFirstRunChecklist(input({ connections: "error", teammates: 1, conversationCount: 3 }));
    expect(c.visible).toBe(false);
  });

  it("a snooze hides it only until it expires", () => {
    const snoozed = deriveFirstRunChecklist(input({ snoozedUntil: NOW + 1000 }));
    expect(snoozed.visible).toBe(false);
    expect(snoozed.snoozed).toBe(true);
    const expired = deriveFirstRunChecklist(input({ snoozedUntil: NOW - 1 }));
    expect(expired.visible).toBe(true);
  });

  it("caps a snooze written far in the future at one snooze window", () => {
    const c = deriveFirstRunChecklist(input({ snoozedUntil: NOW + 100 * FIRST_RUN_SNOOZE_MS }));
    expect(c.visible).toBe(true);
  });

  it("marks connecting not applicable, not unknown, when the org has no connector switched on", () => {
    const c = deriveFirstRunChecklist(input({ connections: "unavailable", conversationCount: 0 }));
    expect(c.items[0].state).toBe("unavailable");
    expect(c.visible).toBe(true);
    expect(c.complete).toBe(false);
  });

  it("is completable without connecting when connecting is not applicable", () => {
    const c = deriveFirstRunChecklist(input({ connections: "unavailable", teammates: 1, conversationCount: 1 }));
    expect(c.complete).toBe(true);
    expect(c.visible).toBe(false);
  });
});

describe("connectorsOffInOrg", () => {
  const BODY =
    "connector authorization is available in your personal workspace only; switch out of the organization workspace to manage it";

  it("recognises the backend's org-management refusal by its code", () => {
    expect(connectorsOffInOrg(new ApiError(BODY, 403, "org_connectors_off"))).toBe(true);
  });

  it("falls back to the sentence only when an older backend sent no code", () => {
    expect(connectorsOffInOrg(new ApiError(BODY, 403))).toBe(true);
    // A different code wins over the sentence: the code is the contract.
    expect(connectorsOffInOrg(new ApiError(BODY, 403, "interactive_session_required"))).toBe(false);
  });

  it("does not treat any other 403, or the same text on another status, as that refusal", () => {
    expect(connectorsOffInOrg(new ApiError("not a member of this organization", 403))).toBe(false);
    expect(connectorsOffInOrg(new ApiError(BODY, 500, "org_connectors_off"))).toBe(false);
    expect(connectorsOffInOrg(new Error(BODY))).toBe(false);
  });
});

describe("ownConversationCount / teammateCount", () => {
  it("counts only the caller's own chats", () => {
    const row = (mine: boolean) => ({ mine }) as unknown as ConversationSummary;
    expect(ownConversationCount([row(true), row(false), row(true)])).toBe(2);
  });

  it("counts everyone but the caller, pending invites included", () => {
    const m = (user_id: string, pending: boolean) => ({ user_id, pending }) as unknown as OrgDirectoryMember;
    expect(teammateCount([m("me", false)], "me")).toBe(0);
    expect(teammateCount([m("me", false), m("them", true)], "me")).toBe(1);
  });
});
