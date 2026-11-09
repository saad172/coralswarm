// `TopicHeader`'s `canEdit` gate (fix/topics-derive-ocean's can_edit follow-up).
//
// THE BUG THIS PINS. `GET /v1/nodes/{id}` authorizes by REACHABILITY;
// `PATCH /v1/nodes/{id}` authorizes by OWNERSHIP against the caller's active
// account (`node_owned_by_account_scoped`, backend/src/routes/nodes.rs's
// `patch_node_impl_scoped`) — a stricter, DIFFERENT predicate. Once the
// topics page could open a promoted team reef at all (the ocean-derivation
// fix this file's sibling PR shipped), it also started rendering that reef's
// rename/re-type "Edit" button — which calls exactly that PATCH — on reefs
// where the call is GUARANTEED to 403. Before that fix the page never
// resolved far enough to show the button; the fix activated a latent one.
//
// `ReefDetail.can_edit` is the server's own answer to "would PATCH accept
// this?", computed from the SAME predicate PATCH enforces. This file checks
// that `TopicHeader` actually wires that field into a real DOM attribute
// (disabled, not merely a decision variable compared to itself) rather than
// rendering the control unconditionally usable or reconstructing the rule
// from something else client-side (`Graph.kind`, `reef.scope`, ...).
//
// WHY THIS RENDERS FOR REAL, IN A `.test.ts` FILE, WITH NO NEW HARNESS. This
// repo deliberately has no component-test harness (see this file's own
// vitest.config.ts and web/lib/requestBudget.test.ts's header) — no jsdom,
// no @testing-library/react, and the vitest `include` glob is `.test.ts`
// only, precisely so nothing here needs a JSX transform. Standing up that
// harness was out of scope for this fix. `react-dom/server`'s
// `renderToStaticMarkup` sidesteps both constraints at once: it needs no DOM
// (SSR-safe in plain Node, matching this project's `environment: "node"`),
// and it's driven with `React.createElement` instead of JSX, so this stays a
// `.test.ts` file the existing glob already picks up. It IS still a real
// render through TopicHeader's actual component function and Button's actual
// prop-spreading — a regression that deletes the `disabled={!canEdit}` wire
// in TopicHeader.tsx fails these assertions, which a test that only checked
// `!canEdit === true` in isolation would not catch.
//
// This is a narrow, one-component tool for one prop, not a precedent for
// testing every screen this way — most of this app's screens are far too
// deep in provider/router context for a bare `createElement` render to be
// worth it, which is exactly why the harness question stays open.

import { describe, expect, it } from "vitest";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { TopicHeader } from "./TopicHeader";
import type { Reef } from "@/lib/api";

function noop() {
  /* unused in these assertions — TopicHeader requires the prop, the render
   * doesn't need it to do anything. */
}

// Two DELIBERATELY DIFFERENT reefs (different id, type, title, scope, counts,
// timestamps) — not one fixture reused with only the boolean flipped — so a
// test that accidentally asserted a value against itself would have nothing
// to accidentally match against.
const EDITABLE_REEF: Reef = {
  node_id: "node-aaaaaaa1-0000-0000-0000-000000000001",
  type: "entity",
  title: "Editable Alpha Topic",
  scope: "team",
  atom_count: 3,
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-02T00:00:00Z",
};

const LOCKED_REEF: Reef = {
  node_id: "node-bbbbbbb2-0000-0000-0000-000000000002",
  type: "decision",
  title: "Locked Beta Topic",
  scope: "org",
  atom_count: 7,
  created_at: "2026-02-05T00:00:00Z",
  updated_at: "2026-02-09T00:00:00Z",
};

function renderHeader(reef: Reef, canEdit: boolean, counts: { sourceCount?: number | null; semanticAtomCount?: number | null; unmappedLegacyChunkCount?: number | null } = {}): string {
  return renderToStaticMarkup(
    createElement(TopicHeader, {
      reef,
      graphName: "Test Workspace",
      atomCount: reef.atom_count,
      sourceCount: 1,
      ...counts,
      editing: false,
      draftTitle: "",
      onDraftTitleChange: noop,
      savingCuration: false,
      curationErr: null,
      onStartEdit: noop,
      onCancelEdit: noop,
      onSaveTitle: noop,
      onPickType: noop,
      onShare: noop,
      // Pinned true in both fixtures so a disabled PROMOTE button (a
      // different prop, `canPromote`) can never be mistaken for the Edit
      // button in the markup match below.
      canPromote: true,
      canEdit,
      onAsk: noop,
    }),
  );
}

/** Isolates the rendered `<button>…Edit…</button>` element specifically —
 *  not just "does `disabled` appear anywhere in the markup", which would
 *  also true-positive on an unrelated disabled Promote/Save button. Fails
 *  loudly (rather than returning an empty match) if the label ever changes,
 *  so a rename doesn't silently make this guard match nothing. */
function editButtonMarkup(html: string): string {
  const m = html.match(/<button[^>]*>Edit<\/button>/);
  if (!m) throw new Error(`no "Edit" button found in rendered markup: ${html}`);
  return m[0];
}

describe("TopicHeader — can_edit gates the Edit affordance", () => {
  it("renders Edit usable when the topic is editable (can_edit: true)", () => {
    const btn = editButtonMarkup(renderHeader(EDITABLE_REEF, true));
    expect(btn).not.toMatch(/\bdisabled(=|>|\/)/);
    expect(btn).not.toMatch(/\btitle=/);
  });

  it("renders Edit disabled — not removed — when the topic is not editable (can_edit: false)", () => {
    const btn = editButtonMarkup(renderHeader(LOCKED_REEF, false));
    expect(btn).toMatch(/\bdisabled=""/);
    // Disabled, never hidden — the control stays a stable landmark and the
    // tooltip explains why it's inert (same idiom as the Promote button's
    // own `canPromote` gate).
    expect(btn).toMatch(/title="[^"]*Only admins can change shared topics[^"]*"/i);
  });
});

describe("TopicHeader — no standalone graph destination", () => {
  it("does not offer View in graph (the canvas is unlisted)", () => {
    expect(renderHeader(EDITABLE_REEF, true)).not.toContain("View in graph");
    expect(renderHeader(LOCKED_REEF, false)).not.toContain("View in graph");
  });
});


describe("TopicHeader evidence counts", () => {
  it("keeps semantic atoms, preserved sources, and unmapped items distinct", () => {
    const html = renderHeader(LOCKED_REEF, false, { sourceCount: 2, semanticAtomCount: 12, unmappedLegacyChunkCount: 3 });
    expect(html).toContain("2 preserved sources");
    expect(html).toContain("12 atoms");
    expect(html).toContain("7 retrieval items");
    expect(html).toContain("3 items have no preserved source mapping");
  });
  it("does not turn missing authoritative counts into zero sources", () => {
    const html = renderHeader(LOCKED_REEF, false, { sourceCount: null, semanticAtomCount: null });
    expect(html).not.toContain("preserved sources");
    expect(html).toContain("7 retrieval items");
  });
});
