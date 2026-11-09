import { pageSectionPath } from "../pageSectionWire";
// Typed client for `GET /v1/home/workspace-activity` (the Home "From your
// workspaces" card). Contract: docs/implementation/workspace-activity-api.md.
// The server picks the headline reef and applies the access rules; this file
// only types the response, fetches it and formats the one-line summary.

export const WORKSPACE_ACTIVITY_MAX_LIMIT = 20;
export const WORKSPACE_ACTIVITY_MAX_SINCE_DAYS = 30;

export type WorkspaceHeadlineKind = "reef_updated" | "reef_added" | "corals_added";

export interface WorkspaceHeadline {
  kind: WorkspaceHeadlineKind;
  /** The busiest reef; null for `corals_added`. */
  reef: { node_id: string; title: string } | null;
  /** New corals on that reef (`corals_added`: in the whole ocean) in the window. */
  coral_count: number;
}

export interface WorkspaceActivityRow {
  graph_id: string;
  name: string;
  /** "org" | "team". Private oceans are never listed. */
  kind: string;
  /** ISO instant of the newest new coral. */
  last_activity_at: string;
  headline: WorkspaceHeadline;
  total_corals_window: number;
}

export interface WorkspaceActivity {
  /** Newest activity first. */
  workspaces: WorkspaceActivityRow[];
}

export interface WorkspaceActivityParams {
  limit?: number;
  sinceDays?: number;
}

export function workspaceActivityPath(params: WorkspaceActivityParams = {}): string {
  const q = new URLSearchParams();
  if (params.limit !== undefined) {
    if (!Number.isInteger(params.limit) || params.limit < 1 || params.limit > WORKSPACE_ACTIVITY_MAX_LIMIT) {
      throw new Error(`limit must be an integer from 1 to ${WORKSPACE_ACTIVITY_MAX_LIMIT}`);
    }
    q.set("limit", String(params.limit));
  }
  if (params.sinceDays !== undefined) {
    if (
      !Number.isInteger(params.sinceDays) ||
      params.sinceDays < 1 ||
      params.sinceDays > WORKSPACE_ACTIVITY_MAX_SINCE_DAYS
    ) {
      throw new Error(`sinceDays must be an integer from 1 to ${WORKSPACE_ACTIVITY_MAX_SINCE_DAYS}`);
    }
    q.set("since_days", String(params.sinceDays));
  }
  const qs = q.toString();
  return pageSectionPath("workspace_activity", Object.fromEntries(new URLSearchParams(qs)));
}

/** The card's one-line summary: `Auth rewrite reef updated · 14 corals`,
 *  `EU launch plan added · 6 corals`, or `14 new corals` with no reef. */
export function workspaceHeadlineText(h: WorkspaceHeadline): string {
  const noun = h.coral_count === 1 ? "coral" : "corals";
  if (h.kind === "corals_added" || !h.reef) return `${h.coral_count} new ${noun}`;
  const n = `${h.coral_count} ${noun}`;
  return h.kind === "reef_added" ? `${h.reef.title} added · ${n}` : `${h.reef.title} reef updated · ${n}`;
}

