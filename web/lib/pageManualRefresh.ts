import type { QueryClient } from "@tanstack/react-query";

interface ManualState { busy: boolean; error: Error | null }
/** One refresh action per canonical resource, shared by all its observers.
 * The last admitted observer owns cancellation; leaving one panel cannot
 * cancel a refresh still observed in another panel. No payload is stored. */
export function createManualRefresh() {
  let value: ManualState = { busy: false, error: null };
  let observers = 0;
  let current: { controller: AbortController; promise: Promise<void> } | null = null;
  const listeners = new Set<() => void>();
  const publish = (next: ManualState) => { value = next; listeners.forEach((listener) => listener()); };
  return {
    snapshot: () => value,
    subscribe: (listener: () => void) => { listeners.add(listener); return () => { listeners.delete(listener); }; },
    observe: () => {
      if (observers === 0 && !current && value.error) publish({ busy: false, error: null });
      observers++;
      return () => { if (--observers === 0) current?.controller.abort(); };
    },
    run: (work: (signal: AbortSignal) => Promise<void>): Promise<void> => {
      if (current) return current.promise;
      if (observers === 0) return Promise.resolve();
      const controller = new AbortController();
      const job = { controller, promise: Promise.resolve() };
      current = job;
      let abort = () => {};
      const cancelled = new Promise<void>((_resolve, reject) => {
        abort = () => reject(controller.signal.reason ?? new DOMException("Aborted", "AbortError"));
        controller.signal.addEventListener("abort", abort, { once: true });
      });
      job.promise = Promise.race([Promise.resolve().then(() => work(controller.signal)), cancelled])
        .catch((error: unknown) => {
          if (current === job && !controller.signal.aborted) publish({ busy: true, error: error instanceof Error ? error : new Error("Could not refresh this section.") });
        }).finally(() => {
          controller.signal.removeEventListener("abort", abort);
          if (current === job) { current = null; publish({ ...value, busy: false }); }
        });
      publish({ busy: true, error: null });
      return job.promise;
    },
  };
}
const stores = new WeakMap<QueryClient, Map<string, ReturnType<typeof createManualRefresh>>>();
export function manualPageRefresh(client: QueryClient, identity: string) {
  let resources = stores.get(client);
  if (!resources) stores.set(client, resources = new Map());
  let resource = resources.get(identity);
  if (!resource) resources.set(identity, resource = createManualRefresh());
  return resource;
}
