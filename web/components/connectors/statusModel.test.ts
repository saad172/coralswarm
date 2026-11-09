import { describe, expect, it } from "vitest";
import type { ConnectorStatus, ConnectorSummary, Problem } from "@/lib/connectorsApi";
import {
  STATUS_VIEW,
  freshnessLine,
  importProgress,
  isUnhealthy,
  primaryAction,
  problemLine,
  problemSentence,
  relativeTime,
  statusView,
} from "./statusModel";

const NOW = new Date("2026-10-29T12:00:00Z");

function summary(over: Partial<ConnectorSummary> = {}): ConnectorSummary {
  return {
    key: "microsoft",
    name: "Microsoft",
    beta: false,
    status: "active",
    sources_on: 4,
    sources_total: 5,
    last_captured_at: "2026-10-29T11:57:00Z",
    importing: null,
    problem: null,
    resume_step: null,
    sources: [],
    ...over,
  };
}

const problem = (code: string, params: Problem["params"] = {}): Problem => ({
  code,
  source: null,
  since: "2026-10-27T12:00:00Z",
  params,
});

describe("status vocabulary", () => {
  it.each<[ConnectorStatus, string, string, string, string, string]>([
    ["not_connected", "Not connected", "muted", "○", "Connect", "secondary"],
    ["setting_up", "Setting up", "muted", "◐", "Continue setup", "primary"],
    ["active", "Active", "ok", "✓", "Manage", "secondary"],
    ["importing", "Importing history", "ok", "↓", "Manage", "secondary"],
    ["needs_attention", "Needs attention", "warn", "!", "Manage", "secondary"],
    ["reconnect_needed", "Reconnect needed", "bad", "×", "Reconnect", "primary"],
  ])("%s", (status, label, tone, glyph, action, variant) => {
    const v = STATUS_VIEW[status];
    expect([v.label, v.tone, v.glyph, v.action.label, v.action.variant]).toEqual([label, tone, glyph, action, variant]);
  });

  it("falls back to not connected for a status it does not know", () => {
    expect(statusView("future_thing" as ConnectorStatus).label).toBe("Not connected");
  });

  it("a recommended, unconnected card takes the brand button; nothing else changes", () => {
    expect(primaryAction("not_connected", true).variant).toBe("brand");
    expect(primaryAction("not_connected").variant).toBe("secondary");
    expect(primaryAction("active", true).variant).toBe("secondary");
    expect(primaryAction("reconnect_needed", true).variant).toBe("primary");
  });

  it("only needs_attention and reconnect_needed are unhealthy", () => {
    const bad = (["not_connected", "setting_up", "active", "importing", "needs_attention", "reconnect_needed"] as const).filter(isUnhealthy);
    expect(bad).toEqual(["needs_attention", "reconnect_needed"]);
  });
});

describe("relativeTime", () => {
  it.each([
    ["2026-10-29T11:59:30Z", "just now"],
    ["2026-10-29T11:57:00Z", "3 min ago"],
    ["2026-10-29T10:00:00Z", "2 h ago"],
    ["2026-10-28T12:00:00Z", "1 day ago"],
    ["2026-10-24T12:00:00Z", "5 days ago"],
    ["2026-10-29T12:05:00Z", "just now"],
    ["not a date", "a while ago"],
  ])("%s -> %s", (iso, want) => expect(relativeTime(iso, NOW)).toBe(want));
});

describe("freshnessLine", () => {
  it("active: sources and last captured", () => {
    expect(freshnessLine(summary(), NOW)).toBe("4 of 5 sources · last captured 3 min ago");
  });

  it("active with nothing captured yet says so", () => {
    expect(freshnessLine(summary({ last_captured_at: null }), NOW)).toBe("4 of 5 sources · nothing captured yet");
  });

  it("a one-source connector reads its scope", () => {
    const slack = summary({
      key: "slack",
      name: "Slack",
      sources_on: 1,
      sources_total: 1,
      sources: [{ scope: { mode: "pick", count: 14, noun: "channels" } } as ConnectorSummary["sources"][number]],
    });
    expect(freshnessLine(slack, NOW)).toBe("14 channels · last captured 3 min ago");
    const one = summary({ ...slack, sources: [{ scope: { mode: "pick", count: 1, noun: "channels" } } as ConnectorSummary["sources"][number]] });
    expect(freshnessLine(one, NOW)).toBe("1 channel · last captured 3 min ago");
    const all = summary({ ...slack, sources: [{ scope: { mode: "everything", count: null, noun: null } } as ConnectorSummary["sources"][number]] });
    expect(freshnessLine(all, NOW)).toBe("Everything I can see · last captured 3 min ago");
  });

  it("importing: window and percent, or just the window when indeterminate", () => {
    const imp = summary({ status: "importing", sources_on: 3, importing: { source: "mail", window_days: 30, percent: 18 } });
    expect(freshnessLine(imp, NOW)).toBe("3 of 5 sources · importing 30 days · 18%");
    expect(freshnessLine({ ...imp, importing: { source: "mail", window_days: 30, percent: null } }, NOW)).toBe("3 of 5 sources · importing 30 days");
    expect(freshnessLine({ ...imp, importing: { source: "mail", window_days: 0, percent: 5 } }, NOW)).toBe("3 of 5 sources · importing history · 5%");
  });

  it("importing progress feeds the Meter", () => {
    const imp = summary({ status: "importing", importing: { source: "mail", window_days: 30, percent: 18 } });
    expect(importProgress(imp)).toEqual({ percent: 18 });
    expect(importProgress({ ...imp, importing: { source: "mail", window_days: 30, percent: null } })).toEqual({ percent: null });
    expect(importProgress(summary())).toBeNull();
  });

  it("setting up says where it stopped", () => {
    expect(freshnessLine(summary({ status: "setting_up", sources_on: 2, resume_step: "scope" }), NOW)).toBe(
      "2 of 5 sources chosen · scope not set yet",
    );
    expect(freshnessLine(summary({ status: "setting_up", sources_on: 0, resume_step: "sources" }), NOW)).toBe(
      "0 of 5 sources chosen · no sources chosen yet · nothing captured",
    );
  });

  it("has no line when not connected or unhealthy", () => {
    for (const status of ["not_connected", "needs_attention", "reconnect_needed"] as const) {
      expect(freshnessLine(summary({ status }), NOW)).toBeNull();
    }
  });
});

describe("problem sentences", () => {
  it.each<[string, Problem["params"], string]>([
    ["credential_invalid", { reason: "ms_grant_expired" }, "Your Microsoft sign-in expired on Oct 27. Reconnect to resume."],
    ["credential_invalid", {}, "Your Microsoft sign-in expired on Oct 27. Reconnect to resume."],
    ["credential_invalid", { reason: "slack_token_revoked" }, "Microsoft revoked access on Oct 27. Reconnect to resume."],
    ["sign_in_expired", {}, "Your Microsoft sign-in expired on Oct 27. Reconnect to resume."],
    ["access_revoked", {}, "Microsoft access was revoked on Oct 27. Reconnect to resume."],
    ["missing_scopes", { count: 2 }, "Microsoft is missing 2 permissions CoralSwarm needs. Reconnect and approve them to resume."],
    ["missing_scopes", { count: 1 }, "Microsoft is missing 1 permission CoralSwarm needs. Reconnect and approve them to resume."],
    ["missing_scopes", {}, "Microsoft is missing some permissions CoralSwarm needs. Reconnect and approve them to resume."],
    ["app_not_installed", {}, "The CoralSwarm app isn't installed in your Microsoft workspace yet. Reconnect to install it."],
    ["tenant_consent_required", {}, "Your Microsoft organization hasn't approved CoralSwarm. Ask an admin to approve it in Microsoft, then retry."],
    ["installation_suspended", {}, "The CoralSwarm installation on Microsoft was suspended on Oct 27. Unsuspend it in Microsoft, then retry."],
    ["capture_unavailable", {}, "Microsoft is connected, but capture isn't working right now. Open Manage to see what to do."],
    ["destination_unreachable", {}, "CoralSwarm couldn't reach some of your Microsoft repositories on Oct 27. Check they still exist and that access is granted."],
    ["sync_failed", {}, "Syncing Microsoft failed on Oct 27. Check Manage for current status. Checking status does not restart capture."],
  ])("%s %j", (code, params, want) => {
    expect(problemSentence(problem(code, params), "Microsoft")).toBe(want);
  });

  it("an unknown code gets the generic sentence and never leaks the code", () => {
    const s = problemSentence(problem("brand_new_failure"), "Jira");
    expect(s).toBe("Jira needs attention since Oct 27. Open Manage to see what happened.");
    expect(s).not.toContain("brand_new_failure");
  });

  it("never names the broker behind the Beta toolkits", () => {
    for (const code of ["capture_unavailable", "sign_in_expired", "access_revoked", "nope"]) {
      expect(problemSentence(problem(code), "Jira").toLowerCase()).not.toContain("composio");
    }
  });

  it("problemLine reads the problem on unhealthy cards only, with a fallback when none came", () => {
    const bad = summary({ status: "reconnect_needed", problem: problem("sign_in_expired") });
    expect(problemLine(bad)).toBe("Your Microsoft sign-in expired on Oct 27. Reconnect to resume.");
    expect(problemLine(summary({ status: "reconnect_needed" }))).toBe("Your Microsoft sign-in stopped working. Reconnect to resume.");
    expect(problemLine(summary({ status: "needs_attention" }))).toBe("Microsoft needs attention. Open Manage to see what happened.");
    expect(problemLine(summary({ status: "active", problem: problem("sync_failed") }))).toBeNull();
  });
});
