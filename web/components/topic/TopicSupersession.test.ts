// `TopicSupersession`'s `canEdit` gate (fix/topics-derive-ocean's can_edit
// follow-up, second instance).
//
// THE BUG THIS PINS. `POST /v1/nodes/{id}/supersede` and `DELETE
// /v1/nodes/{id}/supersede/{other}` authorize with the same account-ownership
// predicate (`node_owned_by_account_scoped`) `patch_node` uses — not the
// reachability check `GET` uses. Once the topics page could open a promoted
// team reef at all (the ocean-derivation fix this file's sibling PR shipped),
// it also started rendering this component's write controls on reefs where
// those calls are guaranteed to fail. Before that fix the page never resolved
// far enough to render this component at all.
//
// Same rendering approach as `TopicHeader.test.ts` (read that file's header
// for the full rationale): `react-dom/server`'s `renderToStaticMarkup`,
// driven by `React.createElement` rather than JSX, so this stays a `.test.ts`
// file the existing no-component-harness vitest config already picks up — no
// new dependencies, no config changes.
//
// WHAT'S TESTABLE FROM A STATIC RENDER, AND WHY. `supersedes`/`supersededBy`
// only populate via the links query, which never fetches during
// `renderToStaticMarkup` (SSR runs no effects) — so the "existing edge" ✕
// remove buttons are never in the initial markup regardless of `canEdit`,
// and aren't a useful assertion target here. The "mark this as
// superseding…" picker (`<select>` + "Mark" button) IS computed straight
// from props at render time (`options = reefs.filter(...)` against the
// initially-empty `linked` set) and is the control every editable topic with
// at least one same-ocean candidate sees on open — that's what's asserted
// below.

import { describe, expect, it, vi } from "vitest";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { QueryClientProvider } from "@tanstack/react-query";
import { makeQueryClient } from "@/lib/queryClient";
import { TopicSupersession } from "./TopicSupersession";

// The links are a cached query (lib/appQueries.ts); a static render never
// fetches, so the provider only has to supply a bearer getter and a scope.
vi.mock("@/lib/providers", () => ({ useApp: () => ({ authReady: false, getAuthToken: () => null, queryScope: "test" }) }));
import type { Reef } from "@/lib/api";

// Two DELIBERATELY DIFFERENT reef/candidate pairs (different ids, types,
// titles, scopes, counts, timestamps) — not one fixture pair reused with
// only the boolean flipped.
const EDITABLE_REEF: Reef = {
  node_id: "node-editable-sup-aaaa0001",
  type: "entity",
  title: "Editable Supersession Subject",
  scope: "team",
  atom_count: 4,
  created_at: "2026-03-01T00:00:00Z",
  updated_at: "2026-03-02T00:00:00Z",
};
const EDITABLE_CANDIDATE: Reef = {
  node_id: "node-editable-sup-aaaa0002",
  type: "entity",
  title: "Editable Candidate Topic",
  scope: "team",
  atom_count: 1,
  created_at: "2026-03-01T00:00:00Z",
  updated_at: "2026-03-01T00:00:00Z",
};

const LOCKED_REEF: Reef = {
  node_id: "node-locked-sup-bbbb0001",
  type: "decision",
  title: "Locked Supersession Subject",
  scope: "org",
  atom_count: 9,
  created_at: "2026-04-01T00:00:00Z",
  updated_at: "2026-04-05T00:00:00Z",
};
const LOCKED_CANDIDATE: Reef = {
  node_id: "node-locked-sup-bbbb0002",
  type: "decision",
  title: "Locked Candidate Topic",
  scope: "org",
  atom_count: 2,
  created_at: "2026-04-01T00:00:00Z",
  updated_at: "2026-04-03T00:00:00Z",
};

function renderSupersession(reef: Reef, candidates: Reef[], canEdit: boolean): string {
  return renderToStaticMarkup(
    createElement(
      QueryClientProvider,
      { client: makeQueryClient() },
      createElement(TopicSupersession, {
        reef,
        reefs: candidates,
        token: null,
        canEdit,
      }),
    ),
  );
}

function selectMarkup(html: string): string {
  const m = html.match(/<select[^>]*>[\s\S]*?<\/select>/);
  if (!m) throw new Error(`no <select> found in rendered markup: ${html}`);
  return m[0];
}

function markButtonMarkup(html: string): string {
  const m = html.match(/<button[^>]*>Mark<\/button>/);
  if (!m) throw new Error(`no "Mark" button found in rendered markup: ${html}`);
  return m[0];
}

describe("TopicSupersession — canEdit gates the supersede controls", () => {
  it("renders the supersede picker usable when the reef is editable (canEdit: true)", () => {
    const html = renderSupersession(EDITABLE_REEF, [EDITABLE_CANDIDATE], true);

    const select = selectMarkup(html);
    expect(select).not.toMatch(/\bdisabled(=|>|\/)/);
    expect(select).not.toMatch(/\btitle=/);

    // The "Mark" button's OWN disabled state is confounded with "no option
    // picked yet" (always true on first render) — its `title`, gated only
    // on `canEdit`, isn't, so that's the unconfounded signal checked here.
    const mark = markButtonMarkup(html);
    expect(mark).not.toMatch(/\btitle=/);
  });

  it("renders the supersede picker disabled — not removed — when the reef is not editable (canEdit: false)", () => {
    const html = renderSupersession(LOCKED_REEF, [LOCKED_CANDIDATE], false);

    const select = selectMarkup(html);
    expect(select).toMatch(/\bdisabled=""/);
    expect(select).toMatch(/title="[^"]*Only admins can change shared topics[^"]*"/i);

    const mark = markButtonMarkup(html);
    expect(mark).toMatch(/\bdisabled=""/);
    expect(mark).toMatch(/title="[^"]*Only admins can change shared topics[^"]*"/i);
  });
});
