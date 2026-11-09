import { describe, expect, it } from "vitest";
import type { ConnectorHealthItem, ConnectorHealthResponse, DirectoryAttribution } from "@/lib/api";
import {
  BACKING_MEMBER_DEPARTED,
  DOMAIN_DIRECTORY,
  GOOGLE_CHAT,
  connectorHealthCopy,
  degradationKey,
  directoryAttributionCopy,
  visibleDegradations,
} from "./connectorHealthModel";

function item(overrides: Partial<ConnectorHealthItem> = {}): ConnectorHealthItem {
  return {
    provider: GOOGLE_CHAT,
    capability: DOMAIN_DIRECTORY,
    condition: BACKING_MEMBER_DEPARTED,
    backing_email: "person@example.com",
    first_observed_at: "2026-09-22T10:00:00Z",
    last_observed_at: "2026-09-22T10:15:00Z",
    detail: { rung: 1, ladder_effect: "auto_add_now_decides_on_rung_2" },
    ...overrides,
  };
}

describe("connectorHealthCopy — D16's directory degradation (CS-446, item 16)", () => {
  it("says what stopped working, what it means, and what fixes it", () => {
    const copy = connectorHealthCopy(item());
    expect(copy.recognised).toBe(true);
    // WHAT — names the capability in words an admin uses, not "rung 1".
    expect(copy.what).toContain("Google Workspace directory");
    // MEANS — the consequence D16 is actually about: the decision is still
    // made, just on weaker evidence. A notice that only said "the directory
    // is gone" would leave an admin with no reason to care.
    expect(copy.means).toContain("weaker check");
    expect(copy.means).toMatch(/join this organization/i);
    // FIXES — something the reader can do, naming the consent that matters.
    expect(copy.remedy).toContain("directory access");
    expect(copy.remedy).toContain("Google Chat");
  });

  it("carries the address of the member whose credential is gone", () => {
    expect(connectorHealthCopy(item()).backingEmail).toBe("person@example.com");
    expect(connectorHealthCopy(item({ backing_email: null })).backingEmail).toBeNull();
  });

  /** FAILS WITHOUT THE FIX. A condition this build has no copy for must
   *  still render, naming the keys it was handed — never disappear, and
   *  never be described with an invented meaning. A connector-health
   *  surface that silently drops what it does not recognise is the same
   *  failure as the `warn!` this whole item replaces, one level up. */
  it("renders an unrecognised condition honestly instead of hiding it", () => {
    const copy = connectorHealthCopy(
      item({ provider: "slack", capability: "roster_walk", condition: "backing_member_departed" }),
    );
    expect(copy.recognised).toBe(false);
    expect(copy.what).toContain("slack");
    expect(copy.what).toContain("roster_walk");
    expect(copy.means).toContain("backing_member_departed");
    expect(copy.remedy.length).toBeGreaterThan(0);
  });

  it("does not claim the directory story for a different capability of the same connector", () => {
    const copy = connectorHealthCopy(item({ capability: "something_else" }));
    expect(copy.recognised).toBe(false);
    expect(copy.what).not.toContain("Workspace directory");
  });
});

function payload(overrides: Partial<ConnectorHealthResponse> = {}): ConnectorHealthResponse {
  return { degradations: [], directory_attribution: null, ...overrides };
}

describe("visibleDegradations / degradationKey", () => {
  it("treats not-loaded and nothing-wrong the same — both render nothing", () => {
    expect(visibleDegradations(null)).toEqual([]);
    expect(visibleDegradations(payload())).toEqual([]);
  });

  it("passes current degradations through", () => {
    const rows = [item()];
    expect(visibleDegradations(payload({ degradations: rows }))).toEqual(rows);
  });

  /** The key is migration 0243's PRIMARY KEY, so two rows that the database
   *  cannot hold at once cannot collide in a list either. */
  it("keys on the same three columns the database keys on", () => {
    expect(degradationKey(item())).toBe("google_chat:domain_directory:backing_member_departed");
    expect(degradationKey(item({ capability: "other" }))).not.toBe(degradationKey(item()));
  });
});

/* ── CS-448 ────────────────────────────────────────────────────────────── */

function attribution(overrides: Partial<DirectoryAttribution> = {}): DirectoryAttribution {
  return {
    provider: GOOGLE_CHAT,
    capability: DOMAIN_DIRECTORY,
    members: [{ email: "person@example.com", disconnected_at: "2026-09-18T09:00:00Z" }],
    ...overrides,
  };
}

describe("directoryAttributionCopy — rung 1 lost to someone who is still here (CS-448)", () => {
  it("names the member, dates the disconnect, and says what it costs", () => {
    const copy = directoryAttributionCopy(attribution());
    expect(copy).not.toBeNull();
    expect(copy!.members).toEqual([
      { email: "person@example.com", disconnectedAt: "2026-09-18T09:00:00Z" },
    ]);
    // WHAT — the capability in an admin's words, plus the fact that the
    // cause is someone who has NOT left. That second half is the whole
    // difference from item 16's notice, and is why a remedy exists at all.
    expect(copy!.what).toContain("Google Workspace directory");
    expect(copy!.what).toMatch(/still here/i);
    // MEANS — the same consequence item 16's copy states, deliberately in
    // the same words: one truth, one voice.
    expect(copy!.means).toContain("weaker check");
    // FIXES — pointed at a person who can actually press the button.
    expect(copy!.remedy).toContain("directory access");
  });

  /** FAILS WITHOUT THE FIX. The claim the surface must never make. The
   *  stored grant is deleted by the disconnect, so nobody can know after
   *  the fact whether it carried `directory.readonly`; copy that asserted
   *  it would be inventing a backer, which is the failure mode this whole
   *  area is built around refusing. */
  it("never claims the named member's grant was the one carrying the scope", () => {
    const copy = directoryAttributionCopy(attribution())!;
    const all = `${copy.what} ${copy.means} ${copy.remedy}`;
    expect(all).not.toMatch(/their grant (was|carried)/i);
    expect(all).not.toMatch(/was the only/i);
    expect(all).not.toMatch(/ran through/i);
  });

  it("describes the scope with CS-431's own words rather than a second copy of them", () => {
    const copy = directoryAttributionCopy(attribution())!;
    // `scopeLabel`/`scopeDescription` come from `reconsentModel`, so the
    // member-facing re-consent prompt and this org-facing notice can never
    // drift about what `directory.readonly` is for.
    expect(copy.scopeLabel).toBe("directory.readonly");
    expect(copy.scopeDescription).toContain("Workspace directory");
  });

  it("reads as plural when several members disconnected", () => {
    const copy = directoryAttributionCopy(
      attribution({
        members: [
          { email: "person@example.com", disconnected_at: "2026-09-18T09:00:00Z" },
          { email: "person@example.com", disconnected_at: "2026-09-12T09:00:00Z" },
        ],
      }),
    )!;
    expect(copy.members).toHaveLength(2);
    expect(copy.what).toMatch(/these members/i);
    expect(copy.remedy).toMatch(/any of them/i);
  });

  /** The three server-side "nothing to say" answers — somebody backs it,
   *  we cannot tell, and nobody ever did — all arrive as `null`, and the
   *  surface renders nothing for each. */
  it("says nothing when the server had nothing to attribute", () => {
    expect(directoryAttributionCopy(null)).toBeNull();
    expect(directoryAttributionCopy(undefined)).toBeNull();
  });

  /** Belt-and-braces: the backend refuses to send an empty list, and a
   *  notice that named nobody would read as "your directory is broken, we
   *  won't say why" — the exact shape of alarm this area avoids. */
  it("refuses to render a notice that names nobody", () => {
    expect(directoryAttributionCopy(attribution({ members: [] }))).toBeNull();
  });
});
