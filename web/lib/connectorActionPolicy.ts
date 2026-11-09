import type { QueryClient } from "@tanstack/react-query";
import { refreshLimits, refreshRecoveryMs } from "./pageRefreshPolicy";

// A finite UI recovery policy, not a provider size limit or scheduling SLA.
// Larger selections resume from the last verified cursor on explicit Retry.
export const SCOPE_PAGES_PER_ACTION = 10;
export const CONNECTOR_PICKER_PATH = "/v1/connectors/{provider}/sources/{source}/scope-items";
export const connectorActionRecoveryMs = () => refreshRecoveryMs(refreshLimits("connector_detail"));
const callbacks = new WeakMap<QueryClient, Map<string, number>>();
export function noteConnectorCallback(client: QueryClient, scope: string, provider: string, deadline: number) {
  let entries = callbacks.get(client);
  if (!entries) callbacks.set(client, entries = new Map());
  entries.set(JSON.stringify([scope, provider]), deadline);
}
export function connectorCallbackDeadline(client: QueryClient, scope: string, provider: string): number | undefined {
  return callbacks.get(client)?.get(JSON.stringify([scope, provider]));
}
