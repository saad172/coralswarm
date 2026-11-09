import { describe, expect, it } from "vitest";
import { LEGACY_SETTINGS_ROUTES, settingsNavFor } from "./settingsNav";

const labels = (role: Parameters<typeof settingsNavFor>[0], loaded = true) =>
  settingsNavFor(role, loaded).map((s) => ({ heading: s.heading, labels: s.items.map((i) => i.label) }));

describe("settingsNavFor", () => {
  it("personal user: three shared pages plus Usage and Billing, no Organization", () => {
    expect(labels("personal")).toEqual([
      { heading: undefined, labels: ["Account", "Connections", "Privacy", "Usage", "Billing"] },
    ]);
  });

  it("org member: three shared pages only, no Usage, no Billing and no org pages", () => {
    expect(labels("org-member")).toEqual([
      { heading: undefined, labels: ["Account", "Connections", "Privacy"] },
    ]);
  });

  it("org admin: shared pages plus the Organization heading with five pages", () => {
    expect(labels("org-admin")).toEqual([
      { heading: undefined, labels: ["Account", "Connections", "Privacy"] },
      { heading: "Organization", labels: ["Members", "Permissions", "Connectors", "Usage", "Billing"] },
    ]);
  });

  it("before the role loads, shows only the pages everyone gets", () => {
    for (const role of ["personal", "org-member", "org-admin"] as const) {
      expect(labels(role, false)).toEqual([
        { heading: undefined, labels: ["Account", "Connections", "Privacy"] },
      ]);
    }
  });

  it("personal Billing lives at /settings/billing", () => {
    const item = settingsNavFor("personal", true)[0].items.find((i) => i.label === "Billing");
    expect(item?.href).toBe("/settings/billing");
  });

  it("Account is the /settings landing page", () => {
    expect(settingsNavFor("personal", true)[0].items[0].href).toBe("/settings");
  });

  it("uses plain words: no ocean, reef or coral anywhere in the rail", () => {
    const text = settingsNavFor("org-admin", true)
      .flatMap((s) => [s.heading ?? "", ...s.items.flatMap((i) => [i.label, i.sub])])
      .join(" ");
    expect(text).not.toMatch(/ocean|reef|coral/i);
  });

  it("every legacy route maps to a route that is in the rail", () => {
    const hrefs = new Set(settingsNavFor("org-admin", true).flatMap((s) => s.items.map((i) => i.href)));
    for (const to of Object.values(LEGACY_SETTINGS_ROUTES)) expect(hrefs.has(to)).toBe(true);
  });
});
