import { typedStored, storedTimestamp } from "./pageSectionWire";
import type { ConnectorDetail, ScopeItemsPage } from "./connectorsApi";
const record = (value: unknown): value is Record<string, unknown> => !!value && typeof value === "object" && !Array.isArray(value);
const text = (value: unknown) => value === null || typeof value === "string";
const count = (value: unknown) => Number.isSafeInteger(value) && Number(value) >= 0;
const optionalCount = (value: unknown) => value === null || count(value);

/** Validate the real certificate and required detail/source structure before
 * authorizing an action-only picker address. Never invent an envelope. */
export function verifiedConnectorChoices(body: unknown, provider: string): Set<string> {
  const stored = typedStored<ConnectorDetail>(body, "connector_detail", { provider });
  if (stored.computed_at === null || stored.stale || stored.refresh_eta !== null) throw new Error("Connector setup is still pending.");
  const p: unknown = stored.payload;
  if (!record(p) || p.key !== provider || typeof p.name !== "string" || typeof p.beta !== "boolean" ||
      !["not_connected", "setting_up", "active", "importing", "needs_attention", "reconnect_needed"].includes(String(p.status)) ||
      !count(p.sources_on) || !count(p.sources_total) || !storedTimestamp(p.last_captured_at) ||
      !(p.importing === null || record(p.importing)) || !(p.problem === null || record(p.problem)) ||
      !(p.resume_step === null || ["sources", "scope", "history"].includes(String(p.resume_step))) ||
      !text(p.account_label) || !storedTimestamp(p.connected_at) || !Array.isArray(p.topics) ||
      !record(p.captures) || !Array.isArray(p.captures.items) || !text(p.captures.next_cursor) ||
      !record(p.watched) || !Array.isArray(p.watched.threads) || !text(p.watched.next_cursor) ||
      !record(p.addin_tokens) || !Array.isArray(p.addin_tokens.tokens) || !Array.isArray(p.sources)) throw new Error("Invalid connector detail.");
  const keys = new Set<string>();
  for (const s of p.sources) {
    if (!record(s) || typeof s.key !== "string" || !s.key || keys.has(s.key) || typeof s.label !== "string" ||
        typeof s.enabled !== "boolean" || typeof s.paused !== "boolean" ||
        !["off", "active", "importing", "paused", "needs_attention", "stopped"].includes(String(s.status)) ||
        !["items", "pick_only", "everything_only"].includes(String(s.scope_shape)) ||
        !(s.scope === null || record(s.scope) && ["pick", "everything"].includes(String(s.scope.mode)) && optionalCount(s.scope.count) && text(s.scope.noun)) ||
        !record(s.history) || !(s.history.supported === true ? count(s.history.max_days) : s.history.supported === false && typeof s.history.reason === "string") ||
        !optionalCount(s.history_days) || !storedTimestamp(s.history_requested_at) || !storedTimestamp(s.setup_completed_at) || !storedTimestamp(s.last_captured_at) ||
        !Array.isArray(s.week) || s.week.length !== 7 || !s.week.every(count) || !(s.backfill === null || record(s.backfill))) throw new Error("Invalid connector source.");
    keys.add(s.key);
  }
  return keys;
}
export function verifiedScopeItems(body: unknown): ScopeItemsPage {
  if (!record(body) || !Array.isArray(body.items) || !body.items.every((item) => record(item) && typeof item.id === "string" && typeof item.label === "string" && text(item.meta) && typeof item.selected === "boolean") ||
      !(body.next_cursor === null || typeof body.next_cursor === "string" && body.next_cursor.length > 0) || !optionalCount(body.total)) throw new Error("Invalid source choices.");
  return body as unknown as ScopeItemsPage;
}
