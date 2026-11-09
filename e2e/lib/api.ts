// The backend's real HTTP API, as a signed-in user. Scenarios seed through
// this (README "Seeding"): never SQL.

import { expect, type APIRequestContext } from "@playwright/test";
import crypto from "node:crypto";
import { API_BASE, FAKE, PORTS, REMOTE, TARGET } from "../stack/config.mjs";
import { sessionToken } from "./clerk";

/** What `seedSource`/`ingest` capture; defaults to a note with nobody on it. */
export interface IngestOpts {
  sourceType?: string;
  participants?: string[];
  /** Slack channel name; a Slack source is titled `#<channelName>`. */
  channelName?: string;
  /** Recorded length of a meeting. */
  durationMs?: number;
}

export interface SseFrame {
  event: string;
  data: any;
}

export function parseSse(raw: string): SseFrame[] {
  return raw
    .split("\n\n")
    .filter((frame) => frame.trim())
    .map((frame) => {
      const lines = frame.split("\n");
      const event = lines.find((l) => l.startsWith("event:"))?.slice(6).trim() ?? "message";
      const data = lines.filter((l) => l.startsWith("data:")).map((l) => l.slice(5).replace(/^ /, "")).join("\n");
      return { event, data: JSON.parse(data) };
    });
}

export class Api {
  constructor(
    private readonly request: APIRequestContext,
    private readonly userId: string,
    private readonly tokenFor: () => Promise<string> = () => sessionToken(userId),
  ) {}

  /** With a token minted by the browser's Clerk (active org included). */
  static withToken(request: APIRequestContext, token: () => Promise<string>): Api {
    return new Api(request, "", token);
  }

  async call(method: "GET" | "POST" | "PUT" | "PATCH" | "DELETE", path: string, data?: unknown, headers: Record<string, string> = {}) {
    const res = await this.request.fetch(`${API_BASE}${path}`, {
      method,
      data,
      headers: { authorization: `Bearer ${await this.tokenFor()}`, ...headers },
    });
    return res;
  }

  async json<T = any>(method: "GET" | "POST" | "PUT" | "PATCH" | "DELETE", path: string, data?: unknown, headers?: Record<string, string>): Promise<T> {
    const res = await this.call(method, path, data, headers);
    const body = await res.text();
    expect(res.ok(), `${method} ${path} -> ${res.status()}: ${body.slice(0, 500)}`).toBeTruthy();
    return (body ? JSON.parse(body) : undefined) as T;
  }

  /** `GET /v1/graphs`. The first authenticated request provisions the
   *  caller's personal account and its "Personal" ocean. */
  async graphs(): Promise<{ id: string; name: string; kind?: string }[]> {
    const body = await this.json("GET", "/v1/graphs");
    return Array.isArray(body) ? body : (body.graphs ?? body.data ?? []);
  }

  async personalGraph() {
    const graphs = await this.graphs();
    const g = graphs.find((x) => x.kind === "personal") ?? graphs[0];
    expect(g, `no personal ocean in ${JSON.stringify(graphs)}`).toBeTruthy();
    return g;
  }

  /**
   * Capture a source the way every client does (`POST /v1/ingest/atom`), then
   * have the e2e-only seeding route write its extraction (one fact per
   * sentence) so Ask can cite it inside the scenario's 30 s. See
   * backend/src/e2e_seed.rs for exactly what that route stands in for.
   */
  async seedSource(graphId: string, content: string, sourceId = `e2e-note-${crypto.randomUUID()}`, opts: IngestOpts = {}) {
    localOnly("the e2e seeding route");
    await this.ingest(graphId, content, sourceId, opts);
    const seeded = await this.json("POST", "/v1/e2e/seed/extraction", { graph_id: graphId, source_type: opts.sourceType ?? "note", source_id: sourceId });
    expect(seeded.semantic_atoms).toBeGreaterThan(0);
    return { sourceId, ...seeded };
  }

  /** Capture a note (or another `opts.sourceType`) through the real ingest route. */
  async ingest(graphId: string, content: string, sourceId = `e2e-note-${crypto.randomUUID()}`, opts: IngestOpts = {}) {
    await this.json("POST", "/v1/ingest/atom", {
      graph_id: graphId,
      source_type: opts.sourceType ?? "note",
      source_id: sourceId,
      content,
      participants: opts.participants ?? [],
      ...(opts.channelName ? { channel_name: opts.channelName } : {}),
      ...(opts.durationMs ? { duration_ms: opts.durationMs } : {}),
      timestamp: new Date().toISOString(),
    });
    return sourceId;
  }

  async askStream(graphId: string, text: string, body: Record<string, unknown> = {}): Promise<SseFrame[]> {
    const res = await this.call("POST", "/v1/queries/stream", { text, library_scope: { graph_ids: [graphId] }, ...body });
    const raw = await res.text();
    expect(res.ok(), `POST /v1/queries/stream -> ${res.status()}: ${raw.slice(0, 500)}`).toBeTruthy();
    return parseSse(raw);
  }

  /**
   * Wait for the REAL managed extraction of a captured note (standing
   * admission sweep, then the extraction model): no seeding. Done when the
   * version reports `done` or already shows retained atoms. The sweep runs every 60 s, so this is the slow
   * path; only `@smoke` uses it.
   */
  async waitForExtraction(graphId: string, sourceId: string, timeoutMs: number) {
    let last = "";
    const extracted = async () => {
      const list = await this.json("GET", `/v1/sources/items?graph_id=${graphId}&source_type=note`);
      const item = (list.items ?? []).find((i: any) => i.source_id === sourceId) ?? (list.items ?? [])[0];
      if (!item) return false;
      if (item.extraction_status === "failed") throw new Error(`extraction failed: ${JSON.stringify(item).slice(0, 300)}`);
      const ev = await this.json("GET", `/v1/sources/evidence?graph_id=${graphId}&source_item_id=${item.source_item_id}`);
      const atoms = (ev.versions ?? []).reduce((n: number, v: any) => n + (v.atoms?.length ?? 0), 0);
      last = `status=${item.extraction_status} atoms=${atoms}`;
      return atoms > 0 || item.extraction_status === "done";
    };
    try {
      await expect.poll(extracted, { timeout: timeoutMs, intervals: [1_000, 2_000], message: `real extraction of ${sourceId} did not finish` }).toBe(true);
    } catch (e) {
      throw new Error(`${(e as Error).message} (last: ${last})`);
    }
  }
}

/**
 * Stripe's half of a completed Checkout: the fake marks the session paid and
 * returns the `checkout.session.completed` event; it is signed with the
 * webhook secret and delivered to the backend's real webhook route, exactly
 * as Stripe would deliver it.
 */
export async function completeStripeCheckout(request: APIRequestContext, sessionId: string) {
  localOnly("the fake Stripe");
  const fake = await request.post(`http://127.0.0.1:${PORTS.fakes}/__stripe/complete/${sessionId}`);
  expect(fake.ok(), await fake.text()).toBeTruthy();
  const payload = await fake.text();
  const t = Math.floor(Date.now() / 1000);
  const v1 = crypto.createHmac("sha256", FAKE.stripeWebhookSecret).update(`${t}.${payload}`).digest("hex");
  const res = await request.post(`${API_BASE}/v1/stripe/webhook`, {
    data: payload,
    headers: { "content-type": "application/json", "stripe-signature": `t=${t},v1=${v1}` },
  });
  expect(res.ok(), `stripe webhook -> ${res.status()}: ${await res.text()}`).toBeTruthy();
}

/** The bearer token Pub/Sub would send with a push, signed by the fake Google key. */
export async function googlePushToken(request: APIRequestContext, claims: Record<string, unknown>): Promise<string> {
  localOnly("the fake Google signing key");
  const res = await request.post(`http://127.0.0.1:${PORTS.fakes}/__google/push-token`, { data: claims });
  expect(res.ok(), await res.text()).toBeTruthy();
  return (await res.json()).token;
}

/** Start (`true`) or end an R2 outage for one ocean: the fakes' S3 proxy
 *  refuses every object write under it with 503 SlowDown. */
export async function r2Outage(request: APIRequestContext, oceanId: string, out: boolean) {
  localOnly("the fake R2 outage");
  const url = `http://127.0.0.1:${PORTS.fakes}/__s3/outage/${oceanId}`;
  const res = out ? await request.post(url) : await request.delete(url);
  expect(res.ok(), await res.text()).toBeTruthy();
}

/** Fakes and the seeding route exist only in the local stack. */
function localOnly(what: string) {
  if (REMOTE) throw new Error(`e2e: ${what} is local-stack only; E2E_TARGET=${TARGET} runs @smoke scenarios that use production routes`);
}
