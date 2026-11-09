import { describe, expect, it } from "vitest";
import {
  CONNECTOR_KEYS,
  CONNECTOR_META,
  KNOWN_PROVIDERS,
  PROVIDER_META,
  connectorBlurb,
  githubConnectorTileEnabled,
  isConnectorKey,
  visibleProviders,
} from "./providerMeta";

/* The GitHub tile's rollout gate. `vitest` never populates `NEXT_PUBLIC_*`,
 * so these drive the pure
 * functions through their arguments rather than asserting against
 * `GITHUB_CONNECTOR_TILE` itself, which would pass vacuously regardless of
 * what the flag actually does. */

describe("githubConnectorTileEnabled", () => {
  it("is enabled only for the literal string \"true\"", () => {
    expect(githubConnectorTileEnabled("true")).toBe(true);
  });

  it.each([undefined, "", "false", "1", "TRUE", " true"])("is disabled for %j", (v) => {
    expect(githubConnectorTileEnabled(v)).toBe(false);
  });
});

describe("visibleProviders", () => {
  it("hides github when the flag is off", () => {
    expect(visibleProviders(KNOWN_PROVIDERS, false)).toEqual(["slack", "microsoft"]);
  });

  it("shows github when the flag is on", () => {
    expect(visibleProviders(KNOWN_PROVIDERS, true)).toEqual(["slack", "microsoft", "github"]);
  });

  it("never hides a non-github provider, regardless of the flag", () => {
    expect(visibleProviders(KNOWN_PROVIDERS, false)).toContain("slack");
    expect(visibleProviders(KNOWN_PROVIDERS, false)).toContain("microsoft");
  });
});

describe("CONNECTOR_META (connectors redesign)", () => {
  it("has exactly the ten connectors, in the API's display order", () => {
    expect([...CONNECTOR_KEYS]).toEqual([
      "slack", "microsoft", "google", "github", "linear", "jira", "confluence", "hubspot", "zendesk", "dart",
    ]);
    expect(Object.keys(CONNECTOR_META).sort()).toEqual([...CONNECTOR_KEYS].sort());
    expect(isConnectorKey("jira")).toBe(true);
    expect(isConnectorKey("zoom")).toBe(false);
  });

  it("gives the toolkits their assigned hues", () => {
    expect(CONNECTOR_META.jira.hue).toBe("teal");
    expect(CONNECTOR_META.confluence.hue).toBe("teal");
    expect(CONNECTOR_META.linear.hue).toBe("plum");
    expect(CONNECTOR_META.hubspot.hue).toBe("amber");
    expect(CONNECTOR_META.zendesk.hue).toBe("pink");
    expect(CONNECTOR_META.dart.hue).toBe("coral");
  });

  it("keeps Slack, Microsoft and GitHub on the hues the old tiles use", () => {
    expect(CONNECTOR_META.slack.hue).toBe(PROVIDER_META.slack.hue);
    expect(CONNECTOR_META.microsoft.hue).toBe(PROVIDER_META.microsoft.hue);
    expect(CONNECTOR_META.github.hue).toBe(PROVIDER_META.github.hue);
  });

  it("writes the captures / never line, and never names the broker", () => {
    expect(connectorBlurb(CONNECTOR_META.slack)).toBe("Messages and threads in channels you pick. Never DMs.");
    expect(connectorBlurb(CONNECTOR_META.github)).toBe("Pull requests, reviews, issues, releases and commits. Never your source code.");
    expect(connectorBlurb(CONNECTOR_META.jira)).toBe("Issues, comments and status changes across your site.");
    for (const m of Object.values(CONNECTOR_META)) {
      expect(JSON.stringify(m).toLowerCase()).not.toContain("composio");
    }
  });

  it("does not widen the old tables: the old page still shows only its own providers", () => {
    expect([...KNOWN_PROVIDERS]).toEqual(["slack", "microsoft", "github"]);
  });
});
