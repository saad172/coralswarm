import { describe, expect, it } from "vitest";
import type { OAuthConnection } from "@/lib/api";
import { connectionForProvider, credentialRevoked, reconsentNotice, scopeDescription, scopeLabel } from "./reconsentModel";

/* Pins the three-way "prompt shown / hidden / unknown" contract CS-431
 * (org-onboarding-from-the-invite.md §3 item 34) requires: this must never
 * guess a re-consent prompt into existence, and it must never hide one the
 * backend has already computed. */

function connection(overrides: Partial<OAuthConnection> = {}): OAuthConnection {
  return {
    provider: "slack",
    provider_workspace_id: "T1",
    scope: "channels:read users:read users:read.email",
    expires_at: null,
    connected_at: "2026-09-01T00:00:00Z",
    missing_scopes: [],
    scopes_unknown: false,
    ...overrides,
  };
}

describe("reconsentNotice", () => {
  it("is null when there is no connection at all", () => {
    expect(reconsentNotice(null)).toBeNull();
  });

  it("is null when nothing is missing", () => {
    expect(reconsentNotice(connection({ missing_scopes: [] }))).toBeNull();
  });

  /// FAILS WITHOUT THE FIX: "never guess" means an unknown scope set must
  /// render nothing, even if `missing_scopes` were ever non-empty alongside
  /// it (the backend never sends that combination — `scope_reconsent_status`
  /// returns `(vec![], true)` — but the model must not rely on the backend
  /// alone to keep that promise).
  it("is null when the stored scope set is unknown, even if missing_scopes is non-empty", () => {
    const notice = reconsentNotice(
      connection({ scopes_unknown: true, missing_scopes: ["users:read.email"] }),
    );
    expect(notice).toBeNull();
  });

  it("shown: carries every missing scope, in the backend's own order", () => {
    const notice = reconsentNotice(
      connection({ missing_scopes: ["users:read.email", "im:write"] }),
    );
    expect(notice).toEqual({ scopes: ["users:read.email", "im:write"] });
  });

  it("shown: Google Chat's fully-qualified scope URL passes through unchanged", () => {
    const scope = "https://www.googleapis.com/auth/directory.readonly";
    const notice = reconsentNotice(
      connection({ provider: "google_chat", missing_scopes: [scope] }),
    );
    expect(notice).toEqual({ scopes: [scope] });
  });
});

describe("scopeLabel", () => {
  it("renders a bare Slack scope as-is", () => {
    expect(scopeLabel("users:read.email")).toBe("users:read.email");
  });

  it("renders a Google scope URL as its last path segment", () => {
    expect(scopeLabel("https://www.googleapis.com/auth/directory.readonly")).toBe(
      "directory.readonly",
    );
  });
});

describe("scopeDescription", () => {
  it("names the known Slack scope's purpose", () => {
    expect(scopeDescription("users:read.email")).toMatch(/match colleagues by email/);
  });

  it("names the known Google Chat scope's purpose", () => {
    expect(scopeDescription("https://www.googleapis.com/auth/directory.readonly")).toMatch(
      /Workspace directory/,
    );
  });

  /// A scope this module has no specific copy for still gets a real
  /// sentence, never a blank string or a thrown error — the "never guess"
  /// rule is about DETECTING a missing scope, not about refusing to
  /// describe one the copy table doesn't yet name.
  it("falls back to a generic sentence for an unrecognised scope", () => {
    expect(scopeDescription("channels:leave").length).toBeGreaterThan(0);
  });
});

describe("connectionForProvider — the row a sync loads", () => {
  /// The backend lists each provider's rows in token-load order (the
  /// caller's own row first, newest first). The tile must read that first
  /// row, not some other row for the same provider: a live member row ahead
  /// of a revoked legacy account-wide one shows no notice, and a revoked
  /// member row ahead of a live legacy one does.
  const live = connection({ provider: "microsoft", provider_workspace_id: "me", credential_invalid_at: null });
  const dead = connection({
    provider: "microsoft",
    provider_workspace_id: "legacy",
    credential_invalid_at: "2026-10-07T09:00:00Z",
    credential_invalid_reason: "ms_refresh_token_inactive",
  });

  it("takes the first listed row for the provider", () => {
    expect(connectionForProvider([connection(), live, dead], "microsoft")).toBe(live);
    expect(credentialRevoked(connectionForProvider([live, dead], "microsoft"))).toBe(false);
    expect(credentialRevoked(connectionForProvider([dead, live], "microsoft"))).toBe(true);
  });

  it("is null when the provider has no row", () => {
    expect(connectionForProvider([connection()], "google")).toBeNull();
  });
});
