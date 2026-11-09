import { afterEach, expect, it, vi } from "vitest";
import { makeQueryClient } from "./queryClient";

const clients: ReturnType<typeof makeQueryClient>[] = [];
function client() { const value = makeQueryClient(); clients.push(value); return value; }
afterEach(() => { for (const value of clients.splice(0)) value.clear(); });
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => { resolve = done; });
  return { promise, resolve };
}

it("ordinary fetches reuse fresh cache, then deduplicate the read required by invalidation", async () => {
  const qc = client(); const next = deferred<string>();
  const queryFn = vi.fn().mockResolvedValueOnce("first").mockImplementation(() => next.promise);
  const options = { queryKey: ["ordinary-cache"], queryFn, staleTime: Infinity };
  await expect(qc.fetchQuery(options)).resolves.toBe("first");
  await expect(qc.fetchQuery(options)).resolves.toBe("first");
  expect(queryFn).toHaveBeenCalledTimes(1);
  await qc.invalidateQueries({ queryKey: options.queryKey });
  const reads = Promise.all([qc.fetchQuery(options), qc.fetchQuery(options)]);
  expect(queryFn).toHaveBeenCalledTimes(2);
  next.resolve("after mutation");
  await expect(reads).resolves.toEqual(["after mutation", "after mutation"]);
  expect(qc.getQueryData(options.queryKey)).toBe("after mutation");
});

it("ordinary ensure returns stale cache immediately and deduplicates its requested background revalidation", async () => {
  const qc = client(); const next = deferred<string>(); const queryFn = vi.fn(() => next.promise);
  const options = { queryKey: ["ordinary-ensure"], queryFn, staleTime: Infinity };
  qc.setQueryData(options.queryKey, "cached");
  await qc.invalidateQueries({ queryKey: options.queryKey });
  await expect(qc.ensureQueryData(options)).resolves.toBe("cached");
  expect(queryFn).not.toHaveBeenCalled();
  await expect(qc.ensureQueryData({ ...options, revalidateIfStale: true })).resolves.toBe("cached");
  expect(queryFn).toHaveBeenCalledTimes(1);
  const joined = qc.fetchQuery(options);
  next.resolve("revalidated");
  await expect(joined).resolves.toBe("revalidated");
  expect(queryFn).toHaveBeenCalledTimes(1);
  expect(qc.getQueryData(options.queryKey)).toBe("revalidated");
});

it("ordinary explicit retry policy and rejection preserve the last verified cache", async () => {
  const qc = client(); const key = ["ordinary-retry"];
  const failure = new Error("temporarily unavailable");
  const queryFn = vi.fn().mockRejectedValueOnce(failure).mockResolvedValueOnce("recovered");
  await expect(qc.fetchQuery({ queryKey: key, queryFn, retry: 1, retryDelay: 0 })).resolves.toBe("recovered");
  expect(queryFn).toHaveBeenCalledTimes(2);
  await expect(qc.fetchQuery({ queryKey: key, staleTime: 0, queryFn: async () => { throw failure; } })).rejects.toBe(failure);
  expect(qc.getQueryData(key)).toBe("recovered");
  await expect(qc.prefetchQuery({ queryKey: ["ordinary-prefetch-failure"], queryFn: async () => { throw failure; } })).resolves.toBeUndefined();
  expect(qc.getQueryData(["ordinary-prefetch-failure"])).toBeUndefined();
});

it("ordinary infinite fetch and ensure retain their page shape and fresh-cache behavior", async () => {
  const qc = client(); const queryFn = vi.fn(async ({ pageParam }: { pageParam: number }) => ({ rows: [pageParam] }));
  const options = { queryKey: ["ordinary-pages"], queryFn, initialPageParam: 7, staleTime: Infinity };
  const expected = { pages: [{ rows: [7] }], pageParams: [7] };
  await expect(qc.ensureInfiniteQueryData(options)).resolves.toEqual(expected);
  await expect(qc.fetchInfiniteQuery(options)).resolves.toEqual(expected);
  await expect(qc.ensureInfiniteQueryData(options)).resolves.toEqual(expected);
  expect(queryFn).toHaveBeenCalledTimes(1);
});
