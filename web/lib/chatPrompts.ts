import type { ActivityEvent } from "./api";
import type { WorkspaceActivityRow } from "./pageSectionTypes/workspaceActivity";
export type ActivityRow = ActivityEvent;
export interface Prompt {
  kind: "reef" | "workspace" | "question";
  label: string;
  hue: "coral" | "plum" | "teal";
  text: string;
}
export const greeting = (hour: number) =>
  hour < 12 ? "Good morning" : hour < 18 ? "Good afternoon" : "Good evening";
const name = (value: string) => {
  const clean = value.trim();
  return clean.length > 64 ? `${clean.slice(0, 63)}…` : clean;
};
export function buildPrompts({
  workspaces,
  activity,
}: {
  workspaces: WorkspaceActivityRow[] | null;
  activity: ActivityRow[] | null;
  hour: number;
}): Prompt[] {
  const prompts: Prompt[] = [];
  const reefs = new Set<string>();
  // Activity has source kinds and IDs, not reef titles. Never turn an ID
  // or an arbitrary payload string into a claimed reef name.
  const sources: Record<string, string> = { slack: "Slack threads", meeting: "meetings", note: "notes" };
  for (const event of activity ?? []) {
    const kind = event.payload.source_type;
    if (event.event_type === "atom_ingested" && typeof kind === "string" && Object.hasOwn(sources, kind)) {
      prompts.push({ kind: "question", label: "Recent activity", hue: "coral", text: `What decisions appear in my recently added ${sources[kind]}?` });
    }
    if (event.event_type === "supersession_created") {
      prompts.push({ kind: "question", label: "Recent activity", hue: "teal", text: "Which of my recent sources have been superseded?" });
    }
  }
  const recentGraphs = new Set((activity ?? []).map((row) => row.graph_id));
  const ordered = [...(workspaces ?? [])].sort(
    (a, b) => Number(recentGraphs.has(b.graph_id)) - Number(recentGraphs.has(a.graph_id)),
  );
  for (const row of ordered) {
    if (row.headline.reef?.title.trim()) reefs.add(name(row.headline.reef.title));
    if (row.name.trim())
      prompts.push({
        kind: "workspace",
        label: name(row.name),
        hue: "plum",
        text: `What changed in ${name(row.name)} this week?`,
      });
  }
  const reef = [...reefs][0];
  if (reef)
    prompts.unshift(
      { kind: "reef", label: reef, hue: "coral", text: `What was decided about ${reef}?` },
      { kind: "reef", label: reef, hue: "teal", text: `Who owns ${reef}?` },
    );
  for (const text of [
    "What changed this week?",
    "What decisions need a follow-up?",
    "Who owns the next steps?",
    "Which sources disagree?",
  ])
    prompts.push({ kind: "question", label: "Explore", hue: "teal", text });
  return prompts.filter((p, i) => prompts.findIndex((q) => q.text === p.text) === i).slice(0, 4);
}
