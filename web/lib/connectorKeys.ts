export const CONNECTOR_KEYS = [
  "slack",
  "microsoft",
  "google",
  "github",
  "linear",
  "jira",
  "confluence",
  "hubspot",
  "zendesk",
  "dart",
] as const;
export type ConnectorKey = (typeof CONNECTOR_KEYS)[number];

export function isConnectorKey(s: string): s is ConnectorKey {
  return (CONNECTOR_KEYS as readonly string[]).includes(s);
}

