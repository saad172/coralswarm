// Typed client for `GET /v1/connectors/status` (docs/implementation/
// connector-status-api.md): one health row per connected connector instance,
// for the Home dashboard's Connectors card and side panel. The backend answers
// 404 while HOME_DASHBOARD_ENABLED is off; that surfaces as a plain ApiError.
//
// The response is validated field by field. A row the client cannot read is an
// error, never silently "healthy": a dashboard that shows 14/14 because a
// state string was new would be worse than one that says it could not load.


export type ConnectorState = "healthy" | "stale" | "failing";
/** `reconnect`: re-run the provider's connect flow. `sync_now`:
 *  `POST /v1/connectors/{provider}/sync`. */
export type ConnectorAction = "reconnect" | "sync_now";

export interface ConnectorStatusRow {
  /** `oauth_tokens.provider` literal, Composio toolkit slug, or `mcp`. */
  provider: string;
  instance_id: string;
  display_name: string;
  /** `chat`, `email`, `calendar`, `meetings`, `docs`, `code`, `tickets`, `agents`, `other`, ... */
  kind_label: string;
  state: ConnectorState;
  /** Last successful sync or ingested event. */
  last_synced_at: string | null;
  last_error_at: string | null;
  /** Human-readable; null when healthy. */
  message: string | null;
  items_30d: number;
  action: ConnectorAction | null;
}

export interface ConnectorStatusSummary {
  total: number;
  healthy: number;
  stale: number;
  failing: number;
}

export interface ConnectorStatusResponse {
  /** Failing first, then stale, then healthy by 30-day volume. */
  connectors: ConnectorStatusRow[];
  summary: ConnectorStatusSummary;
}

const STATES: readonly string[] = ["healthy", "stale", "failing"];
const ACTIONS: readonly string[] = ["reconnect", "sync_now"];

const record = (v: unknown): v is Record<string, unknown> => !!v && typeof v === "object" && !Array.isArray(v);
const text = (v: unknown): v is string => typeof v === "string";
const nullableDate = (v: unknown): v is string | null => v === null || (text(v) && Number.isFinite(Date.parse(v)));
const count = (v: unknown): v is number => typeof v === "number" && Number.isSafeInteger(v) && v >= 0;

function validRow(v: unknown): v is ConnectorStatusRow {
  return (
    record(v) &&
    text(v.provider) &&
    text(v.instance_id) &&
    text(v.display_name) &&
    text(v.kind_label) &&
    text(v.state) &&
    STATES.includes(v.state) &&
    nullableDate(v.last_synced_at) &&
    nullableDate(v.last_error_at) &&
    (v.message === null || text(v.message)) &&
    count(v.items_30d) &&
    (v.action === null || (text(v.action) && ACTIONS.includes(v.action)))
  );
}

/** Validate a `GET /v1/connectors/status` body. Throws when it is malformed or
 *  its summary disagrees with its rows. */
export function parseConnectorStatus(body: unknown): ConnectorStatusResponse {
  if (!record(body) || !Array.isArray(body.connectors) || !body.connectors.every(validRow)) {
    throw new Error("Connector status could not be read.");
  }
  const s = body.summary;
  if (!record(s) || !count(s.total) || !count(s.healthy) || !count(s.stale) || !count(s.failing)) {
    throw new Error("Connector status could not be read.");
  }
  const rows = body.connectors as ConnectorStatusRow[];
  const n = (state: ConnectorState) => rows.filter((r) => r.state === state).length;
  if (s.total !== rows.length || s.healthy !== n("healthy") || s.stale !== n("stale") || s.failing !== n("failing")) {
    throw new Error("Connector status could not be read.");
  }
  return { connectors: rows, summary: { total: s.total, healthy: s.healthy, stale: s.stale, failing: s.failing } };
}

