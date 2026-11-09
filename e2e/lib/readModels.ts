import { expect } from "@playwright/test";
import type { Api } from "./api";
import manifest from "../../docs/plans/page-load-standard/l4-model-manifest.json" with { type: "json" };

export type ReadModelName = keyof typeof manifest;

export function refreshPath(path: string): string {
  const [route, query] = path.split("?");
  return `${route}/refresh${query ? `?${query}` : ""}`;
}

/** Real API + real worker only. Never fabricates an envelope or intercepts a response. */
export async function waitForComputed(api: Api, path: string, refresh = true, ready: (payload: any) => boolean = () => true, timeout = 120_000) {
  if (refresh) expect((await api.call("POST", refreshPath(path))).status()).toBe(202);
  let stored: any;
  await expect.poll(async () => {
    stored = await api.json("GET", path);
    expect(stored.model).toEqual(expect.any(String));
    const expected = manifest[stored.model as ReadModelName];
    expect(expected, `registered model ${stored.model}`).toBeDefined();
    expect(stored.version).toBe(expected.version);
    return stored.computed_at !== null && stored.stale === false && ready(stored.payload);
  }, { timeout, intervals: [250, 500, 1000] }).toBe(true);
  return stored;
}

/**
 * Read-your-write: the payload of the first computed answer after the caller's
 * own writes, once `until` holds. A connector write bumps the control revision,
 * which re-keys the model, so the answer computed before it is no longer
 * addressed and any computed answer post-dates the write. (A content-plane
 * reducing write does not re-key: its last answer is served `stale: true`
 * until the recompute, so callers checking such a write pass `until`.) `stale`
 * is not required: a later capture or sync dirties the model again with the
 * 30 s debounce while the payload already shows the write. It never POSTs a
 * refresh, so a write that does not re-key its model returns the old payload
 * and the caller's assertion fails instead of a forced recompute passing it.
 */
export async function freshPayload<P = any>(api: Api, path: string, until: (payload: P) => boolean = () => true, timeout = 15_000): Promise<P> {
  let payload: P | undefined;
  await expect.poll(async () => {
    const stored = await api.json("GET", path);
    const expected = manifest[stored.model as ReadModelName];
    expect(expected, `registered model ${stored.model}`).toBeDefined();
    expect(stored.version).toBe(expected.version);
    payload = stored.payload;
    return stored.computed_at !== null && until(stored.payload);
  }, { timeout, intervals: [250, 500, 1000] }).toBe(true);
  return payload as P;
}

/** A route mock's answer for a stored page read: computed, current, registered. */
export function computedEnvelope(model: ReadModelName, payload: unknown) {
  return { model, version: manifest[model].version, payload, computed_at: new Date().toISOString(), stale: false, refresh_eta: null };
}

/** A route mock's answer to `POST <read>/refresh`: accepted, recompute due soon. */
export function refreshAccepted() {
  return { status: 202, json: { refresh_eta: new Date(Date.now() + 5_000).toISOString() } };
}
