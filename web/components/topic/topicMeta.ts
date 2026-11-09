import type { Hue } from "@/components/ui";
import type { EntityKind, ReefType } from "@/lib/api";

export interface TopicKindMeta {
  glyph: string;
  label: string;
  plural: string;
  hue: Hue;
}

/** Library / topic-detail kind — person, org, topic, project. */
export const ENTITY_KIND_META: Record<EntityKind, TopicKindMeta> = {
  person: { glyph: "●", label: "person", plural: "people", hue: "pink" },
  org: { glyph: "●", label: "org", plural: "orgs", hue: "plum" },
  topic: { glyph: "●", label: "topic", plural: "topics", hue: "teal" },
  project: { glyph: "●", label: "project", plural: "projects", hue: "coral" },
};

export const ENTITY_KINDS: EntityKind[] = ["person", "org", "topic", "project"];

/** Legacy nodes.type taxonomy — Workspaces and older rails still use this. */
export const TOPIC_META: Record<ReefType, TopicKindMeta> = {
  entity: { glyph: "●", label: "entity", plural: "entities", hue: "pink" },
  decision: { glyph: "◆", label: "decision", plural: "decisions", hue: "coral" },
  thread: { glyph: "▤", label: "thread", plural: "threads", hue: "amber" },
  reference: { glyph: "↗", label: "reference", plural: "references", hue: "plum" },
};

export function kindForeground(hue: Hue): string {
  if (hue === "coral") return "var(--brand-2)";
  if (hue === "amber") return "var(--warn)";
  return `var(--hue-${hue})`;
}

export function entityKindOf(reef: { entity_type?: EntityKind | null }): EntityKind {
  return reef.entity_type ?? "topic";
}
