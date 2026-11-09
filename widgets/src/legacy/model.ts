import { type EvidenceItem } from "../answerUi";
import { record } from "./bridge";
import { sourceTypeColor, sourceTypeLabel, type Tokens } from "../theme";
import { describeEvent } from "../eventDisplay";
import type { ActivityEvent } from "../types";

export interface LegacyResult {
  title: string;
  answer?: string;
  sources: EvidenceItem[];
  notice?: string;
  working?: boolean;
  taskId?: string;
  fallback?: string;
}
const text = (value: unknown): string | undefined => typeof value === "string" ? value : undefined;
const rows = (value: unknown): Record<string, unknown>[] => Array.isArray(value) ? value.map(record).filter((item): item is Record<string, unknown> => !!item) : [];
function source(item: Record<string, unknown>, index: number, t: Tokens, numbered = false): EvidenceItem {
  const kind = text(item.source_type);
  return {
    id: `${text(item.atom_id) ?? "source"}:${index}`, number: numbered ? index + 1 : undefined,
    title: text(item.title)?.trim() || text(item.content)?.trim() || text(item.snippet)?.trim() || text(item.atom_id) || "Untitled coral",
    excerpt: text(item.content) ?? text(item.snippet) ?? text(item.excerpt),
    accent: sourceTypeColor(t, kind),
    provenance: [
      { label: "Type", value: sourceTypeLabel(kind) },
      ...[ ["Context", item.channel_name ?? item.project], ["Author", item.author_name], ["Timestamp", item.timestamp], ["Coral", item.atom_id] ].flatMap(([label, value]) => typeof value === "string" && value ? [{ label: String(label), value }] : []),
      ...(item.body_omitted === true ? [{ label: "Excerpt", value: "Full coral was not supplied" }] : []),
    ],
  };
}

/** Normalize recognized historical outputs without asserting a modern schema.
 * Any unfamiliar value still has the original complete, escaped JSON view. */
export function legacyResult(value: unknown, t: Tokens): LegacyResult {
  const data = record(value);
  const error = record(data?.error);
  if (data?.status === "working") return { title: "Waiting for a result", working: true, taskId: text(data.task_id), sources: [] };
  if (data?.status === "cancelled") return { title: "Request cancelled", notice: "The tool call was cancelled.", sources: [] };
  if (data?.status === "failed" || error) return { title: "Something went wrong", notice: text(error?.message) ?? "The task failed with no error detail.", sources: [] };
  if (data && (typeof data.answer === "string" || data.mode === "no_answer" || Array.isArray(data.citation_details))) {
    const sources = rows(data.citation_details).map((item, index) => source(item, index, t, true));
    const unreachable = Array.isArray(data.unreachable_oceans) ? data.unreachable_oceans.length : 0;
    return { title: "CoralSwarm answer", answer: text(data.answer), sources, notice: unreachable ? `${unreachable} ocean${unreachable === 1 ? "" : "s"} unreachable.` : !data.answer && !sources.length ? "No answer or sources were supplied." : undefined };
  }
  if (Array.isArray(data?.results)) return { title: "Search corals", sources: rows(data.results).map((item, index) => source(item, index, t)), notice: data.results.length ? undefined : "No matching corals were supplied." };
  if (Array.isArray(data?.events)) {
    const sources = rows(data.events).map((item, index): EvidenceItem => {
      const event: ActivityEvent = { id: text(item.id) ?? String(index), user_id: text(item.user_id) ?? "", graph_id: text(item.graph_id) ?? null, event_type: text(item.event_type) ?? "activity", payload: record(item.payload) ?? {}, payload_truncated: item.payload_truncated === true, created_at: text(item.created_at) ?? "" };
      const display = describeEvent(event);
      return { id: `${event.id}:${index}`, title: display.body, excerpt: JSON.stringify(event.payload, null, 2), accent: display.badgeColor(t), provenance: [{ label: "Event", value: event.event_type }, { label: "Timestamp", value: event.created_at || "Not supplied" }, ...(event.payload_truncated ? [{ label: "Payload", value: "Truncated by the server" }] : [])] };
    });
    return { title: "Recent activity", sources, notice: !sources.length ? "No recent activity was supplied." : data.has_more === true ? "More activity is available in CoralSwarm." : undefined };
  }
  if (typeof data?.reef_id === "string") {
    const digest = record(data.digest);
    const sources = rows(digest?.excerpts).map((item, index) => source(item, index, t));
    return { title: text(data.title) || "Reef detail", answer: text(digest?.summary), sources, fallback: JSON.stringify(value, null, 2) };
  }
  return { title: "CoralSwarm result", sources: [], fallback: JSON.stringify(value, null, 2) ?? String(value) };
}
