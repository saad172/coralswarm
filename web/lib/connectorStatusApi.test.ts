import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { connectorStatusQueryOptions } from "./appQueries";
import { ApiError } from "./api";
import { parseConnectorStatus } from "./pageSectionTypes/connectorStatus";

const wire = vi.hoisted(() => ({ fetch: vi.fn() }));
vi.mock("./transport", async (original) => ({ ...await original<typeof import("./transport")>(), guardedFetch: wire.fetch }));

const row = (over: Record<string, unknown> = {}) => ({
  provider: "slack",
  instance_id: "slack:T1:account",
  display_name: "Slack",
  kind_label: "chat",
  state: "healthy",
  last_synced_at: "2026-10-29T10:00:00Z",
  last_error_at: null,
  message: null,
  items_30d: 12,
  action: null,
  ...over,
});
const body = (rows: Record<string, unknown>[], summary?: Record<string, number>) => ({
  connectors: rows,
  summary: summary ?? {
    total: rows.length,
    healthy: rows.filter((r) => r.state === "healthy").length,
    stale: rows.filter((r) => r.state === "stale").length,
    failing: rows.filter((r) => r.state === "failing").length,
  },
});

beforeEach(() => wire.fetch.mockReset());
afterEach(() => vi.restoreAllMocks());

describe("parseConnectorStatus", () => {
  it("accepts a well-formed response", () => {
    const failing = row({ state: "failing", message: "Access revoked", action: "reconnect", last_error_at: "2026-10-26T00:00:00Z" });
    const parsed = parseConnectorStatus(body([failing, row()]));
    expect(parsed.summary).toEqual({ total: 2, healthy: 1, stale: 0, failing: 1 });
    expect(parsed.connectors[0].action).toBe("reconnect");
  });

  it.each([
    ["unknown state", { state: "degraded" }],
    ["unknown action", { action: "restart" }],
    ["negative count", { items_30d: -1 }],
    ["bad date", { last_synced_at: "yesterday" }],
    ["missing message", { message: undefined }],
  ])("rejects %s", (_name, over) => {
    expect(() => parseConnectorStatus(body([row(over)]))).toThrow("could not be read");
  });

  it("rejects a summary that disagrees with its rows", () => {
    expect(() => parseConnectorStatus(body([row()], { total: 1, healthy: 0, stale: 0, failing: 1 }))).toThrow(
      "could not be read",
    );
  });

  it("rejects a non-object body", () => {
    expect(() => parseConnectorStatus(null)).toThrow("could not be read");
    expect(() => parseConnectorStatus({ connectors: "x" })).toThrow("could not be read");
  });
});

describe("stored connector status", () => {
  async function read(token: string, signal = new AbortController().signal) {
    const options = connectorStatusQueryOptions({ scope: "fixture", getAuthToken: () => token });
    const value = options.select(await options.queryFn({ signal }));
    if (!value) throw new Error("Expected computed fixture");
    return value;
  }
  it("GETs the actual envelope with bearer, critical priority and no-store", async () => {
    wire.fetch.mockResolvedValue(new Response(JSON.stringify({ model: "connector_status", version: 1, payload: body([row()]), computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null })));
    const signal = new AbortController().signal;
    const out = await read("tok", signal);
    const [path, init, options] = wire.fetch.mock.calls[0];
    expect(path).toBe("/v1/connectors/status");
    expect(init).toMatchObject({ cache: "no-store", headers: { Authorization: "Bearer tok" } });
    expect(options).toMatchObject({ priority: "critical", signal });
    expect(out.summary.total).toBe(1);
  });
  it("preserves feature-off status", async () => {
    wire.fetch.mockImplementation(() => Promise.resolve(new Response("{}", { status: 404 })));
    await expect(read("tok")).rejects.toMatchObject({ status: 404 });
    await expect(read("tok")).rejects.toBeInstanceOf(ApiError);
  });
  it("refuses an empty token without a request", async () => {
    await expect(read("")).rejects.toThrow("Not authenticated");
    expect(wire.fetch).not.toHaveBeenCalled();
  });
});
