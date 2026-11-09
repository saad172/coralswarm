import { CancelledError, QueryObserver, type Query, type QueryKey, type QueryObserverOptions } from "@tanstack/react-query";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { apiRequest, ApiError } from "./api";
import { makeQueryClient } from "./queryClient";
import { pagePayloadQuery, pageSectionQuery, type Stored } from "./pageSections";
import { refusePageRead } from "./pageReadAuthority";
import { meQueryOptions, orgSelfserveEnabledQueryOptions } from "./appQueries";
import { plainPageSectionQuery } from "./pageShell";

vi.mock("./providers", () => ({ useApp: vi.fn() }));
vi.mock("./api", async (actual) => ({ ...await actual<typeof import("./api")>(), apiRequest: vi.fn() }));
const request = vi.mocked(apiRequest);
const envelope = (payload: string[], changes: Partial<Stored<string[]>> = {}): Stored<string[]> => ({
  model: "journal_gist", version: 1, payload, computed_at: "2026-10-31T00:00:00Z",
  stale: false, refresh_eta: null, ...changes,
});
type Factory = "plain" | "stored" | "payload";
function optionsFor(kind: Factory, getToken = () => "actor-A", scope = "account-A", queryKey?: QueryKey): QueryObserverOptions<unknown> & { queryKey: QueryKey } {
  const selected = kind === "payload" ? pagePayloadQuery<string[]>("journal_gist", scope, {}, getToken, { queryKey }) : undefined;
  const options = kind === "plain"
    ? pageSectionQuery<string[]>("/v1/me", scope, {}, getToken, { plain: true, queryKey })
    : selected ?? pageSectionQuery<string[]>("journal_gist", scope, {}, getToken, { queryKey });
  // Keep the scenario driven by explicit reads, without polling or mount reads.
  return { queryKey: options.queryKey, queryFn: options.queryFn, meta: options.meta, staleTime: options.staleTime,
    structuralSharing: options.structuralSharing,
    retry: options.retry, select: selected ? (value: unknown) => selected.select(value as Stored<string[]>) : undefined,
    enabled: false, refetchInterval: false, refetchOnMount: false };
}
function payload(value: unknown): unknown {
  return value && typeof value === "object" && "computed_at" in value
    ? (value as Stored<string[]>).computed_at === null ? undefined : (value as Stored<string[]>).payload
    : value;
}
let client: ReturnType<typeof makeQueryClient>;
beforeEach(() => { client = makeQueryClient(); request.mockReset(); });
afterEach(() => { client.clear(); });
function required<T>(value: T | undefined): T {
  if (value === undefined) throw new Error("Expected the query/observer to exist");
  return value;
}
function readIdentity(options: QueryObserverOptions<unknown>): string {
  const identity = options.meta?.readIdentity;
  if (typeof identity !== "string") throw new Error("Expected a canonical read identity");
  return identity;
}
async function atFulfilledBeforePublication(query: Query, action: () => Promise<unknown> | void) {
  for (let step = 0; step < 100; step++) {
    if ((query.promise as Promise<unknown> & { status?: string })?.status === "fulfilled") break;
    await Promise.resolve();
  }
  expect((query.promise as Promise<unknown> & { status?: string }).status).toBe("fulfilled");
  expect(query.state.fetchStatus).toBe("fetching");
  // Execute in THIS microtask. Returning first would let core publish before
  // the caller resumes, silently weakening the exact race being reproduced.
  await action();
}

describe("authority survives the initiating retryer's last observer leaving", () => {
  it.each(["remove", "clear"])("%s invalidates joined completions when the old query instance leaves the app client", async (mode) => {
    const options = { ...optionsFor("plain"), staleTime: 0 };
    request.mockResolvedValue("private A");
    await client.fetchQuery(options);
    const callers = Array.from({ length: 3 }, () => client.fetchQuery(options).then((data) => ({ data }), (error: unknown) => ({ error })));
    const query = required(client.getQueryCache().find({ queryKey: options.queryKey, exact: true }));
    let nextAccount: Promise<unknown> | undefined;
    await atFulfilledBeforePublication(query, () => {
      if (mode === "clear") client.clear();
      else client.removeQueries({ queryKey: options.queryKey, exact: true });
      request.mockResolvedValue("private B");
      nextAccount = client.fetchQuery(optionsFor("plain", () => "actor-B", "account-B"));
    });
    for (const outcome of await Promise.all(callers)) expect("error" in outcome && outcome.error).toBeInstanceOf(CancelledError);
    await expect(nextAccount).resolves.toBe("private B");
    expect(client.getQueryData(options.queryKey)).toBeUndefined();
  });

  it.each(["fetch", "ensure"])("%s indexes a bootstrap-seeded page before returning cached data", async (method) => {
    const options = optionsFor("plain");
    client.setQueryData(options.queryKey, "verified bootstrap seed");
    const pending = (method === "fetch" ? client.fetchQuery(options) : client.ensureQueryData(options))
      .then((data) => ({ data }), (error: unknown) => ({ error }));
    await refusePageRead(client, readIdentity(options), new ApiError("refused", 403));
    const outcome = await pending;
    expect("error" in outcome && outcome.error).toBeInstanceOf(CancelledError);
    expect(client.getQueryData(options.queryKey)).toBeUndefined();
    request.mockResolvedValue("reverified");
    await expect(client.fetchQuery(options)).resolves.toBe("reverified");
  });

  for (const kind of ["plain", "stored", "payload"] as const) {
    it.each(["accounts", "clients"])(`${kind}: joined callers keep %s isolated`, async (separation) => {
      const a = { ...optionsFor(kind), staleTime: 0 };
      const b = { ...optionsFor(kind, () => "actor-B", "account-B"), staleTime: 0 };
      const other = separation === "clients" ? makeQueryClient() : client;
      const privateA = kind === "plain" ? "private A" : envelope(["private A"]);
      const privateB = kind === "plain" ? "private B" : envelope(["private B"]);
      request.mockImplementation(async (_path, token) => token === "actor-A" ? privateA : privateB);
      try {
        await client.fetchQuery(a);
        await other.fetchQuery(b);
        const refused = Array.from({ length: 3 }, () => client.fetchQuery(a).then((data) => ({ data }), (error: unknown) => ({ error })));
        const admitted = [other.fetchQuery(b), other.fetchQuery(b)];
        const query = required(client.getQueryCache().find({ queryKey: a.queryKey, exact: true }));
        await atFulfilledBeforePublication(query, () => refusePageRead(client, readIdentity(a), new ApiError("refused A", 403)));
        for (const outcome of await Promise.all(refused)) expect("error" in outcome && outcome.error).toBeInstanceOf(CancelledError);
        await expect(Promise.all(admitted)).resolves.toEqual([privateB, privateB]);
        expect(client.getQueryData(a.queryKey)).toBeUndefined();
        expect(other.getQueryData(b.queryKey)).toEqual(privateB);
      } finally { if (other !== client) other.clear(); }
    });

    it.each(["identical", "different"])(`${kind}: joined old attempts cannot displace overlapping %s recovery`, async (version) => {
      const options = { ...optionsFor(kind), staleTime: 0 };
      const original = kind === "plain" ? "original primitive" : envelope(["original"]);
      const reverified = version === "identical" ? original : kind === "plain" ? "new primitive" : envelope(["new"]);
      request.mockResolvedValue(original);
      await client.fetchQuery(options);
      const callers = Array.from({ length: 4 }, () => client.fetchQuery(options).then(
        (data) => ({ data, cached: client.getQueryData(options.queryKey) }), (error: unknown) => ({ error }),
      ));
      const query = required(client.getQueryCache().find({ queryKey: options.queryKey, exact: true }));
      let recovery: Promise<unknown> | undefined;
      await atFulfilledBeforePublication(query, () => {
        const refusal = refusePageRead(client, readIdentity(options), new ApiError("refused", 403));
        request.mockResolvedValue(reverified);
        recovery = client.fetchQuery(options);
        return refusal;
      });
      for (const outcome of await Promise.all(callers)) {
        if ("data" in outcome) {
          expect(outcome.data).toEqual(reverified);
          expect(outcome.cached).toEqual(reverified); // A real new certificate must already exist.
        } else expect(outcome.error).toBeInstanceOf(CancelledError);
      }
      await expect(recovery).resolves.toEqual(reverified);
      expect(client.getQueryData(options.queryKey)).toEqual(reverified);
    });
  }

  it.each(["fetch cache", "ensure cache", "ensure revalidate", "ensure missing"])("%s validates each imperative completion", async (mode) => {
    const options = { ...optionsFor("plain"), staleTime: mode === "ensure revalidate" ? 0 : Infinity };
    request.mockResolvedValue("private");
    await client.fetchQuery(options);
    if (mode === "ensure missing") client.removeQueries({ queryKey: options.queryKey, exact: true });
    const callers = Array.from({ length: 3 }, () => (mode === "fetch cache" ? client.fetchQuery(options)
      : client.ensureQueryData({ ...options, revalidateIfStale: mode === "ensure revalidate" })).then(
      (data) => ({ data }), (error: unknown) => ({ error }),
    ));
    const query = required(client.getQueryCache().find({ queryKey: options.queryKey, exact: true }));
    if (mode === "ensure missing") await atFulfilledBeforePublication(query, () => refusePageRead(client, readIdentity(options), new ApiError("refused", 403)));
    else await refusePageRead(client, readIdentity(options), new ApiError("refused", 403));
    for (const outcome of await Promise.all(callers)) expect("error" in outcome && outcome.error).toBeInstanceOf(CancelledError);
    expect(client.getQueryData(options.queryKey)).toBeUndefined();
    request.mockResolvedValue("reverified");
    await expect(client.ensureQueryData(options)).resolves.toBe("reverified");
  });

  it.each(["fetch", "ensure"])("infinite %s joins use the app completion boundary", async (method) => {
    const options = { ...plainPageSectionQuery<string>("/v1/me", "account-A", {}, () => "actor-A"),
      queryKey: ["infinite", method], initialPageParam: 0, getNextPageParam: () => undefined, staleTime: 0 };
    request.mockResolvedValue("private page");
    await client.fetchInfiniteQuery(options);
    if (method === "ensure") client.removeQueries({ queryKey: options.queryKey, exact: true });
    const callers = Array.from({ length: 3 }, () => (method === "fetch" ? client.fetchInfiniteQuery(options)
      : client.ensureInfiniteQueryData(options)).then((data) => ({ data }), (error: unknown) => ({ error })));
    const query = required(client.getQueryCache().find({ queryKey: options.queryKey, exact: true }));
    await atFulfilledBeforePublication(query, () => refusePageRead(client, options.meta.readIdentity, new ApiError("refused", 403)));
    for (const outcome of await Promise.all(callers)) expect("error" in outcome && outcome.error).toBeInstanceOf(CancelledError);
    expect(client.getQueryData(options.queryKey)).toBeUndefined();
    request.mockResolvedValue("reverified page");
    await expect(client.ensureInfiniteQueryData(options)).resolves.toEqual({ pages: ["reverified page"], pageParams: [0] });
  });

  it("prefetch swallows authority cancellation without exposing data to a joined fetch", async () => {
    const options = { ...optionsFor("plain"), staleTime: 0 };
    request.mockResolvedValue("private");
    await client.fetchQuery(options);
    const prefetched = client.prefetchQuery(options);
    const joined = client.fetchQuery(options).then((data) => ({ data }), (error: unknown) => ({ error }));
    const query = required(client.getQueryCache().find({ queryKey: options.queryKey, exact: true }));
    await atFulfilledBeforePublication(query, () => refusePageRead(client, readIdentity(options), new ApiError("refused", 403)));
    await expect(prefetched).resolves.toBeUndefined();
    const outcome = await joined;
    expect("error" in outcome && outcome.error).toBeInstanceOf(CancelledError);
    expect(client.getQueryData(options.queryKey)).toBeUndefined();
  });

  it("ordinary queries retain dedupe and evaluate functional staleTime once per call", async () => {
    const queryFn = vi.fn(async () => "ordinary");
    const staleTime = vi.fn(() => 0);
    const options = { queryKey: ["ordinary"], queryFn, staleTime };
    await expect(Promise.all([client.fetchQuery(options), client.fetchQuery(options)])).resolves.toEqual(["ordinary", "ordinary"]);
    expect(queryFn).toHaveBeenCalledTimes(1);
    expect(staleTime).toHaveBeenCalledTimes(2);
  });

  for (const kind of ["plain", "stored", "payload"] as const) {
    it.each([2, 5])(`${kind}: every deduplicated imperative caller is fenced (%s callers)`, async (count) => {
      const options = { ...optionsFor(kind), staleTime: 0 };
      const value = kind === "plain" ? "private primitive" : envelope(["private"]);
      request.mockResolvedValue(value);
      await client.fetchQuery(options);
      const callers = Array.from({ length: count }, () => client.fetchQuery(options).then(
        (data) => ({ data }), (error: unknown) => ({ error }),
      ));
      const query = required(client.getQueryCache().find({ queryKey: options.queryKey, exact: true }));
      await atFulfilledBeforePublication(query, () => refusePageRead(client, readIdentity(options), new ApiError("refused", 403)));
      const outcomes = await Promise.all(callers);
      expect(request).toHaveBeenCalledTimes(2); // Warm read plus one shared attempt.
      for (const outcome of outcomes) {
        expect(outcome).toHaveProperty("error");
        expect("error" in outcome && outcome.error).toBeInstanceOf(CancelledError);
      }
      expect(client.getQueryData(options.queryKey)).toBeUndefined();
      request.mockRejectedValue(new ApiError("unavailable", 503));
      await expect(client.fetchQuery(options)).rejects.toMatchObject({ status: 503 });
      expect(client.getQueryData(options.queryKey)).toBeUndefined();
      request.mockResolvedValue(value); // Same exact primitive/object must reverify.
      await expect(client.fetchQuery(options)).resolves.toEqual(value);
    });
  }

  it.each(["distinct keys", "distinct clients"])("identical primitives cannot borrow publication authority across %s", async (separation) => {
    const a = optionsFor("plain", undefined, "account-A", ["primitive-A"]);
    const b = separation === "distinct clients" ? a : optionsFor("plain", undefined, "account-A", ["primitive-B"]);
    const other = separation === "distinct clients" ? makeQueryClient() : client;
    request.mockResolvedValue("identical private primitive");
    try {
      await client.fetchQuery(a);
      await other.fetchQuery(b);
      const old = client.fetchQuery({ ...a, staleTime: 0 }).then((value) => ({ value }), (error: unknown) => ({ error }));
      const query = required(client.getQueryCache().find({ queryKey: a.queryKey, exact: true }));
      let recovery: Promise<unknown> | undefined;
      await atFulfilledBeforePublication(query, () => {
        const refusal = refusePageRead(client, readIdentity(a), new ApiError("refused", 403));
        recovery = other.fetchQuery({ ...b, staleTime: 0 });
        return refusal;
      });
      const outcome = await old;
      expect(outcome).toHaveProperty("error");
      expect("error" in outcome && outcome.error).toBeInstanceOf(CancelledError);
      await recovery;
      expect(client.getQueryData(a.queryKey)).toBeUndefined();
      expect(other.getQueryData(b.queryKey)).toBe("identical private primitive");
      expect(await client.fetchQuery({ ...a, staleTime: 0 })).toBe("identical private primitive");
    } finally { if (other !== client) other.clear(); }
  });

  it.each(["plain", "stored", "payload"] as const)("%s: a synchronous fresh-descriptor remount cannot drop final-publication certification", async (kind) => {
    const options = optionsFor(kind);
    const response = kind === "plain" ? ["private"] : envelope(["private"]);
    request.mockResolvedValue(response);
    await client.fetchQuery(options);
    const observer = new QueryObserver(client, options);
    let remount: QueryObserver<unknown> | undefined;
    let stop = () => {};
    let stopRemount = () => {};
    const exposed: unknown[] = [];
    stop = observer.subscribe((result) => {
      if (result.data === undefined && result.error && !remount) {
        stop();
        remount = new QueryObserver(client, optionsFor(kind));
        stopRemount = remount.subscribe((next) => { if (next.data !== undefined) exposed.push(next.data); });
      }
    });
    const query = required(client.getQueryCache().find({ queryKey: options.queryKey, exact: true }));
    const read = observer.refetch();
    try {
      await atFulfilledBeforePublication(query, () => refusePageRead(client, readIdentity(options), new ApiError("refused", 403)));
      await read;
      expect(remount).toBeDefined();
      expect(exposed).toEqual([]);
      expect(required(remount).getCurrentResult().data).toBeUndefined();
      request.mockRejectedValue(new ApiError("unavailable", 503));
      await required(remount).refetch();
      expect(exposed).toEqual([]);
      request.mockResolvedValue(response);
      await required(remount).refetch();
      expect(payload(required(remount).getCurrentResult().data)).toEqual(["private"]);
    } finally { stop(); stopRemount(); }
  });

  it.each(["direct plain", "primitive true", "primitive false"])("appQueries %s: fulfilled data is checked at final cache publication", async (kind) => {
    const context = { scope: "account-A", getAuthToken: () => "actor-A" };
    const source = kind === "direct plain" ? meQueryOptions(context) : orgSelfserveEnabledQueryOptions(context);
    const options = { ...source, enabled: false, refetchOnMount: false } as QueryObserverOptions<unknown> & { queryKey: QueryKey };
    const wire = kind === "direct plain" ? { name: "private" } : { selfserve_signup_enabled: kind === "primitive true" };
    request.mockResolvedValue(wire);
    await client.fetchQuery(options);
    const observer = new QueryObserver(client, options);
    let stop = () => {};
    stop = observer.subscribe((result) => { if (result.data === undefined && result.error) stop(); });
    const query = required(client.getQueryCache().find({ queryKey: options.queryKey, exact: true }));
    const read = observer.refetch();
    await atFulfilledBeforePublication(query, () => refusePageRead(client, readIdentity(options), new ApiError("refused", 403)));
    await read;
    expect(client.getQueryData(options.queryKey)).toBeUndefined();
    const remount = new QueryObserver(client, options);
    expect(remount.getCurrentResult().data).toBeUndefined();
    stop = remount.subscribe(() => {});
    try {
      request.mockRejectedValue(new ApiError("unavailable", 503));
      await remount.refetch();
      expect(remount.getCurrentResult().data).toBeUndefined();
      request.mockResolvedValue(wire); // Identical primitive/object values must recover.
      await remount.refetch();
      expect(remount.getCurrentResult().data).toEqual(kind === "direct plain" ? wire : kind === "primitive true");
      expect(remount.getCurrentResult().status).toBe("success");
    } finally { stop(); }
  });

  for (const kind of ["plain", "stored", "payload"] as const) {
    it.each(kind === "plain" ? ["different result", "identical result"] : ["different result", "identical result", "withholding"])(`${kind}: old fulfilled work cannot overwrite overlapping recovery with %s`, async (resultKind) => {
      const options = optionsFor(kind);
      const response = (value: string[]) => kind === "plain" ? value : envelope(value);
      const original = response(["original"]);
      const recovered = resultKind === "identical result" ? original : resultKind === "withholding"
        ? envelope([], { computed_at: null, stale: true, refresh_eta: "2026-10-31T00:00:30Z" }) : response(["new authority"]);
      request.mockResolvedValue(original);
      await client.fetchQuery(options);
      const observer = new QueryObserver(client, options);
      let fenced = false;
      let recovering = false;
      const forbidden: unknown[] = [];
      const stop = observer.subscribe((result) => {
        if (result.data === undefined && result.error) fenced = true;
        if (fenced && !recovering && result.data !== undefined) forbidden.push(result.data);
      });
      const query = required(client.getQueryCache().find({ queryKey: options.queryKey, exact: true }));
      const old = observer.refetch();
      let recovery: Promise<unknown> | undefined;
      try {
        await atFulfilledBeforePublication(query, () => {
          const refusal = refusePageRead(client, readIdentity(options), new ApiError("refused", 403));
          request.mockResolvedValue(recovered);
          // A fresh descriptor/remount may replace Query.options while the
          // obsolete core continuation is still queued.
          const remounted = optionsFor(kind);
          observer.setOptions(remounted);
          recovering = true;
          recovery = observer.refetch();
          return refusal;
        });
        await Promise.all([old, recovery]);
        expect(fenced).toBe(true);
        expect(forbidden).toEqual([]);
        expect(payload(observer.getCurrentResult().data)).toEqual(payload(recovered));
        expect(observer.getCurrentResult().status).toBe("success");
        request.mockRejectedValue(new ApiError("unavailable", 503));
        await observer.refetch();
        expect(payload(observer.getCurrentResult().data)).toEqual(payload(recovered));
      } finally { stop(); }
    });
  }

  it.each(["plain", "stored", "payload"] as const)("%s: refusal after retryer fulfillment prevents queued success publication", async (kind) => {
    const options = optionsFor(kind);
    const response = kind === "plain" ? ["private"] : envelope(["private"]);
    request.mockResolvedValue(response);
    await client.fetchQuery(options);
    const observer = new QueryObserver(client, options);
    let fenced = false;
    const postFence: unknown[] = [];
    let stop = () => {};
    stop = observer.subscribe((result) => {
      if (result.data === undefined && result.error) { fenced = true; stop(); }
      if (fenced) postFence.push(payload(result.data));
    });
    const query = required(client.getQueryCache().find({ queryKey: options.queryKey, exact: true }));
    const read = observer.refetch();
    await atFulfilledBeforePublication(query, () => refusePageRead(client, readIdentity(options), new ApiError("refused after fulfillment", 403)));
    await read;
    expect(fenced).toBe(true);
    expect(postFence.every((value) => value === undefined)).toBe(true);
    expect(client.getQueryData(options.queryKey)).toBeUndefined();
    const remount = new QueryObserver(client, options);
    expect(remount.getCurrentResult().data).toBeUndefined();
    stop = remount.subscribe((result) => { postFence.push(payload(result.data)); });
    try {
      request.mockRejectedValue(new ApiError("unavailable", 503));
      await remount.refetch();
      expect(remount.getCurrentResult().data).toBeUndefined();
      expect(postFence.every((value) => value === undefined)).toBe(true);
      request.mockResolvedValue(kind === "plain" ? ["verified recovery"] : envelope(["verified recovery"]));
      await remount.refetch();
      expect(payload(remount.getCurrentResult().data)).toEqual(["verified recovery"]);
    } finally { stop(); }
  });

  const refusals = [401, 403, 404, "invalid certificate", "invalid JSON", "account change"] as const;
  for (const kind of ["plain", "stored", "payload"] as const) {
    it.each(kind === "plain" ? refusals.filter((refusal) => refusal !== "invalid certificate") : refusals)(`${kind}: %s stays fenced through remount and 503 until verified recovery`, async (refusal) => {
      let token = "actor-A";
      const options = optionsFor(kind, () => token);
      const response = (value: string[]) => kind === "plain" ? value : envelope(value);
      request.mockResolvedValue(response(["private"]));
      await client.fetchQuery(options);
      const first = new QueryObserver(client, options);
      const last = new QueryObserver(client, options);
      expect(payload(first.getCurrentResult().data)).toEqual(["private"]);
      let fenced = false;
      let stopFirst = () => {};
      let stopLast = () => {};
      stopFirst = first.subscribe((result) => {
        if (result.data === undefined && result.error) stopFirst();
      });
      stopLast = last.subscribe((result) => {
        if (result.data === undefined && result.error) { fenced = true; stopLast(); }
      });
      if (typeof refusal === "number") request.mockRejectedValue(new ApiError("refused", refusal));
      else if (refusal === "invalid JSON") request.mockRejectedValue(new SyntaxError("Invalid JSON"));
      else if (refusal === "invalid certificate" && kind !== "plain") request.mockResolvedValue(envelope(["invalid"], { version: 999 }));
      else request.mockImplementation(async () => { token = "actor-B"; return response(["wrong account"]); });
      await first.refetch();
      expect(fenced).toBe(true);
      expect(client.getQueryCache().find({ queryKey: options.queryKey, exact: true })?.getObserversCount()).toBe(0);
      expect(client.getQueryData(options.queryKey)).toBeUndefined();

      const remount = new QueryObserver(client, options);
      expect(remount.getCurrentResult().data).toBeUndefined();
      const stop = remount.subscribe(() => {});
      try {
        request.mockRejectedValue(new ApiError("unavailable", 503));
        await remount.refetch();
        expect(remount.getCurrentResult().error).toMatchObject({ status: 503 });
        expect(remount.getCurrentResult().data).toBeUndefined();
        request.mockResolvedValue(response(["verified recovery"]));
        await remount.refetch();
        expect(payload(remount.getCurrentResult().data)).toEqual(["verified recovery"]);
        expect(remount.getCurrentResult().status).toBe("success");
      } finally { stop(); }
    });
  }

  it.each(["stored", "payload"] as const)("%s: withholding stays fenced after last-observer removal, remount and 503", async (kind) => {
    const options = optionsFor(kind);
    request.mockResolvedValue(envelope(["private"]));
    await client.fetchQuery(options);
    const observer = new QueryObserver(client, options);
    let withheld = false;
    let stop = () => {};
    stop = observer.subscribe((result) => {
      if (result.status === "success" && payload(result.data) === undefined) { withheld = true; stop(); }
    });
    request.mockResolvedValue(envelope([], { computed_at: null, stale: true, refresh_eta: "2026-10-31T00:00:30Z" }));
    await observer.refetch();
    expect(withheld).toBe(true);
    expect(payload(client.getQueryData(options.queryKey))).toBeUndefined();
    const remount = new QueryObserver(client, options);
    expect(payload(remount.getCurrentResult().data)).toBeUndefined();
    stop = remount.subscribe(() => {});
    try {
      request.mockRejectedValue(new ApiError("unavailable", 503));
      await remount.refetch();
      expect(remount.getCurrentResult().error).toMatchObject({ status: 503 });
      expect(payload(remount.getCurrentResult().data)).toBeUndefined();
      request.mockResolvedValue(envelope(["verified recovery"]));
      await remount.refetch();
      expect(payload(remount.getCurrentResult().data)).toEqual(["verified recovery"]);
    } finally { stop(); }
  });

  it.each(["refusal", "withholding"])("%s fences alternate stored/payload keys and an abort-ignoring peer", async (fence) => {
    const stored = optionsFor("stored", undefined, "account-A", ["stored"]);
    const selected = optionsFor("payload", undefined, "account-A", ["payload"]);
    request.mockResolvedValue(envelope(["private"]));
    await client.fetchQuery(stored);
    await client.fetchQuery(selected);
    const initiating = new QueryObserver(client, stored);
    const peer = new QueryObserver(client, selected);
    let stopInitiating = () => {};
    let stopPeer = () => {};
    stopInitiating = initiating.subscribe((result) => { if (payload(result.data) === undefined) stopInitiating(); });
    stopPeer = peer.subscribe((result) => { if (payload(result.data) === undefined) stopPeer(); });
    let finish!: (value: unknown) => void;
    let started!: () => void;
    const ready = new Promise<void>((resolve) => { started = resolve; });
    const late = new Promise<unknown>((resolve) => { finish = resolve; });
    let peerSignal: AbortSignal | undefined;
    request.mockImplementationOnce((_path, _token, _init, _retry, options) => { peerSignal = options?.signal; started(); return late; });
    const pendingPeer = peer.refetch();
    await ready;
    if (fence === "refusal") request.mockRejectedValue(new ApiError("refused", 403));
    else request.mockResolvedValue(envelope([], { computed_at: null, stale: true, refresh_eta: "2026-10-31T00:00:30Z" }));
    await initiating.refetch();
    expect(peerSignal?.aborted).toBe(true);
    finish(envelope(["late private"]));
    await pendingPeer;
    for (const options of [stored, selected]) {
      const query = client.getQueryCache().find({ queryKey: options.queryKey, exact: true });
      expect(query?.getObserversCount()).toBe(0);
      expect(query?.state.fetchStatus).toBe("idle");
      expect(payload(query?.state.data)).toBeUndefined();
    }
    const remount = new QueryObserver(client, selected);
    expect(payload(remount.getCurrentResult().data)).toBeUndefined();
    const stop = remount.subscribe(() => {});
    try {
      request.mockRejectedValue(new ApiError("unavailable", 503));
      await remount.refetch();
      expect(payload(remount.getCurrentResult().data)).toBeUndefined();
      request.mockResolvedValue(envelope(["verified recovery"]));
      await remount.refetch();
      expect(remount.getCurrentResult().data).toEqual(["verified recovery"]);
      expect(payload(client.getQueryData(stored.queryKey))).toEqual(["verified recovery"]);
    } finally { stop(); }
  });

  it.each(["plain", "stored", "payload"] as const)("%s: navigating to another account and back cannot restore refused authority", async (kind) => {
    const accountA = optionsFor(kind);
    const accountB = optionsFor(kind, () => "actor-B", "account-B");
    const response = (value: string[]) => kind === "plain" ? value : envelope(value);
    request.mockResolvedValueOnce(response(["private A"])).mockResolvedValueOnce(response(["private B"]));
    await client.fetchQuery(accountA);
    await client.fetchQuery(accountB);
    const observer = new QueryObserver(client, accountA);
    let stop = () => {};
    stop = observer.subscribe((result) => {
      if (result.data === undefined && result.error) {
        stop();
        observer.setOptions(accountB);
      }
    });
    request.mockRejectedValue(new ApiError("refused A", 403));
    await observer.refetch();
    expect(payload(observer.getCurrentResult().data)).toEqual(["private B"]);
    observer.setOptions(accountA);
    expect(observer.getCurrentResult().data).toBeUndefined();
    stop = observer.subscribe(() => {});
    try {
      request.mockRejectedValue(new ApiError("unavailable", 503));
      await observer.refetch();
      expect(observer.getCurrentResult().error).toMatchObject({ status: 503 });
      expect(observer.getCurrentResult().data).toBeUndefined();
      expect(payload(client.getQueryData(accountB.queryKey))).toEqual(["private B"]);
      request.mockResolvedValue(response(["verified A"]));
      await observer.refetch();
      expect(payload(observer.getCurrentResult().data)).toEqual(["verified A"]);
    } finally { stop(); }
  });

  for (const kind of ["plain", "stored", "payload"] as const) {
    it.each(kind === "plain" ? ["refusal"] : ["refusal", "withholding"])(`${kind}: reentrant peer refetch during %s publication cannot restore its private snapshot`, async (fence) => {
      const a = optionsFor(kind, undefined, "account-A", ["reentrant-A"]);
      const b = optionsFor(kind, undefined, "account-A", ["reentrant-B"]);
      const response = (value: string[]) => kind === "plain" ? value : envelope(value);
      request.mockResolvedValue(response(["private"]));
      await client.fetchQuery(a);
      await client.fetchQuery(b);
      const first = new QueryObserver(client, a);
      const second = new QueryObserver(client, b);
      let refetchPeer: Promise<unknown> | undefined;
      let stopFirst = () => {};
      let stopSecond = () => {};
      stopFirst = first.subscribe((result) => {
        if (payload(result.data) === undefined && !refetchPeer) {
          // The peer is still warm when the first fence notification arrives.
          expect(payload(client.getQueryData(b.queryKey))).toEqual(["private"]);
          request.mockReturnValue(new Promise(() => {}));
          refetchPeer = second.refetch();
          stopFirst();
        }
      });
      stopSecond = second.subscribe((result) => { if (payload(result.data) === undefined) stopSecond(); });
      if (fence === "refusal") request.mockRejectedValue(new ApiError("refused", 403));
      else request.mockResolvedValue(envelope([], { computed_at: null, stale: true, refresh_eta: "2026-10-31T00:00:30Z" }));
      await first.refetch();
      expect(refetchPeer).toBeDefined();
      await refetchPeer;
      expect(client.getQueryCache().find({ queryKey: b.queryKey, exact: true })?.getObserversCount()).toBe(0);
      expect(payload(client.getQueryData(b.queryKey))).toBeUndefined();
      if (fence === "refusal") {
        expect(client.getQueryState(b.queryKey)?.error).toMatchObject({ status: 403 });
        expect(request).toHaveBeenCalledTimes(3); // No reentrant token/transport read.
      }
      const remount = new QueryObserver(client, b);
      expect(payload(remount.getCurrentResult().data)).toBeUndefined();
      const stop = remount.subscribe(() => {});
      try {
        request.mockRejectedValue(new ApiError("unavailable", 503));
        await remount.refetch();
        expect(remount.getCurrentResult().error).toMatchObject({ status: 503 });
        expect(payload(remount.getCurrentResult().data)).toBeUndefined();
        request.mockResolvedValue(response(["verified recovery"]));
        await remount.refetch();
        expect(payload(remount.getCurrentResult().data)).toEqual(["verified recovery"]);
      } finally { stop(); }
    });
  }

  it.each(["plain", "stored", "payload"] as const)("%s: cancellation abort callbacks cannot open an unfenced retryer", async (kind) => {
    const a = optionsFor(kind, undefined, "account-A", ["abort-A"]);
    const b = optionsFor(kind, undefined, "account-A", ["abort-B"]);
    const response = (value: string[]) => kind === "plain" ? value : envelope(value);
    request.mockResolvedValue(response(["private"]));
    await client.fetchQuery(a);
    await client.fetchQuery(b);
    const first = new QueryObserver(client, a);
    const second = new QueryObserver(client, b);
    let restarted: Promise<unknown> | undefined;
    let stopFirst = () => {};
    let stopSecond = () => {};
    stopFirst = first.subscribe((result) => { if (result.data === undefined) stopFirst(); });
    stopSecond = second.subscribe((result) => { if (result.data === undefined) stopSecond(); });
    let started!: () => void;
    const ready = new Promise<void>((resolve) => { started = resolve; });
    request.mockImplementationOnce((_path, _token, _init, _retry, options) => {
      options?.signal?.addEventListener("abort", () => { restarted = second.refetch(); }, { once: true });
      started();
      return new Promise(() => {});
    });
    const pending = second.refetch();
    await ready;
    request.mockRejectedValue(new ApiError("refused", 403));
    await first.refetch();
    expect(restarted).toBeDefined();
    await Promise.all([pending, restarted]);
    expect(client.getQueryData(b.queryKey)).toBeUndefined();
    expect(client.getQueryState(b.queryKey)).toMatchObject({ status: "error", fetchStatus: "idle", error: { status: 403 } });
    expect(request).toHaveBeenCalledTimes(4);
  });

  it.each(["plain", "stored", "payload"] as const)("%s: the refusal window covers new keys created during notification", async (kind) => {
    const a = optionsFor(kind, undefined, "account-A", ["existing"]);
    const added = optionsFor(kind, undefined, "account-A", ["added-during-fence"]);
    const response = (value: string[]) => kind === "plain" ? value : envelope(value);
    request.mockResolvedValue(response(["private"]));
    await client.fetchQuery(a);
    const first = new QueryObserver(client, a);
    let newRead: Promise<unknown> | undefined;
    let newObserver: QueryObserver<unknown> | undefined;
    let stopFirst = () => {};
    let stopNew = () => {};
    stopFirst = first.subscribe((result) => {
      if (result.data === undefined && !newObserver) {
        newObserver = new QueryObserver(client, added);
        stopNew = newObserver.subscribe((next) => { if (next.data === undefined) stopNew(); });
        newRead = newObserver.refetch();
        stopFirst();
      }
    });
    request.mockRejectedValue(new ApiError("refused", 403));
    await first.refetch();
    expect(newRead).toBeDefined();
    await newRead;
    expect(client.getQueryData(added.queryKey)).toBeUndefined();
    expect(client.getQueryState(added.queryKey)).toMatchObject({ status: "error", fetchStatus: "idle", error: { status: 403 } });
    expect(request).toHaveBeenCalledTimes(2);
    const remount = new QueryObserver(client, added);
    expect(remount.getCurrentResult().data).toBeUndefined();
    const stop = remount.subscribe(() => {});
    try {
      request.mockRejectedValue(new ApiError("unavailable", 503));
      await remount.refetch();
      expect(remount.getCurrentResult().data).toBeUndefined();
      request.mockResolvedValue(response(["verified recovery"]));
      await remount.refetch();
      expect(payload(remount.getCurrentResult().data)).toEqual(["verified recovery"]);
    } finally { stop(); stopNew(); }
  });
});
