import { expect, it } from "vitest";
import { buildPrompts, greeting } from "./chatPrompts";
import type { WorkspaceActivityRow } from "./pageSectionTypes/workspaceActivity";
const row: WorkspaceActivityRow = {
  graph_id: "g",
  name: "Design",
  kind: "team",
  last_activity_at: "",
  total_corals_window: 1,
  headline: { kind: "reef_updated", reef: { node_id: "r", title: 'Launch "one"' }, coral_count: 1 },
};
it("fills deterministic templates and falls back for empty and null data", () => {
  const input = { workspaces: [row], activity: null, hour: 9 };
  expect(buildPrompts(input)).toEqual(buildPrompts(input));
  expect(buildPrompts(input).map((p) => p.text)).toContain('What was decided about Launch "one"?');
  expect(buildPrompts(input).map((p) => p.text)).toContain("What changed in Design this week?");
  expect(buildPrompts({ workspaces: [], activity: [], hour: 9 })).toEqual(
    buildPrompts({ workspaces: null, activity: null, hour: 9 }),
  );
  expect(buildPrompts(input)).toHaveLength(4);
});
it("bounds names and greets by local hour", () => {
  expect(
    buildPrompts({ workspaces: [{ ...row, name: "a".repeat(200) }], activity: null, hour: 9 }).find(
      (p) => p.kind === "workspace",
    )?.label,
  ).toHaveLength(64);
  expect([0, 11, 12, 17, 18, 23].map(greeting)).toEqual([
    "Good morning",
    "Good morning",
    "Good afternoon",
    "Good afternoon",
    "Good evening",
    "Good evening",
  ]);
});

it("uses personal activity without shared workspaces or invented reef titles", () => {
  const prompts = buildPrompts({ workspaces: null, hour: 9, activity: [{ id: "a", event_type: "atom_ingested", graph_id: "private", user_id: "u", created_at: "", payload: { source_type: "note", reef_title: "Fabricated" } }] });
  expect(prompts[0].text).toBe("What decisions appear in my recently added notes?");
  expect(JSON.stringify(prompts)).not.toContain("Fabricated");
});
