import { expect, it } from "vitest";
import { CONNECTOR_KEYS } from "@/components/settings/providerMeta";
import { legacyConnectTarget } from "./legacyConnect";

it("the four sign-in connectors take the old authorize redirect, with the surface their old cards pass", () => {
  expect(legacyConnectTarget("slack")).toEqual({ kind: "oauth", provider: "slack", surface: "slack_member" });
  expect(legacyConnectTarget("google")).toEqual({ kind: "oauth", provider: "google", surface: "workspace" });
  expect(legacyConnectTarget("microsoft")).toEqual({ kind: "oauth", provider: "microsoft" });
  expect(legacyConnectTarget("github")).toEqual({ kind: "oauth", provider: "github" });
});

it("every Beta toolkit goes to the toolkit connect page", () => {
  for (const key of ["linear", "jira", "confluence", "hubspot", "zendesk", "dart"]) {
    expect(legacyConnectTarget(key)).toEqual({ kind: "href", href: `/connectors/composio?toolkit=${key}` });
  }
});

it("every connector has a target", () => {
  for (const key of CONNECTOR_KEYS) expect(legacyConnectTarget(key)).toBeTruthy();
});
