import { CancelledError, replaceEqualDeep, type Query, type QueryClient, type QueryKey } from "@tanstack/react-query";
import { ApiError } from "./api";
import { StoredCertificateError } from "./pageSectionWire";
import { sameCredentialScope } from "./credentialScope";

export async function pageReadToken(getToken: () => string | null | Promise<string | null>, publicRead = false) {
  try {
    const token = await getToken();
    if (!token && !publicRead) throw new Error("Not authenticated");
    return token;
  } catch (error) {
    throw new StoredCertificateError(error instanceof Error ? error.message : "The page account changed. Verify the current account.");
  }
}
export function verifyPageReadToken(original: string | null, current: string | null) {
  if (original !== current && !(original && current && sameCredentialScope(original, current)))
    throw new StoredCertificateError("The page account changed. Verify the current account.");
}

export interface PageReadContext { signal: AbortSignal; client?: QueryClient; queryKey?: QueryKey }
const generations = new WeakMap<QueryClient, Map<string, number>>();
const publishingRefusals = new WeakMap<QueryClient, Map<string, Error>>();
type ReadCertificate = Map<string, number>;
interface Publication { value: unknown; certificate: ReadCertificate; promise?: Promise<unknown> }
interface QueryAuthority {
  client: QueryClient;
  attempts: WeakMap<Promise<unknown>, ReadCertificate>;
  publications: Publication[];
  manual?: Publication;
  cached?: Publication;
}
const queryAuthorities = new WeakMap<object, QueryAuthority>();
interface ImperativeAttempt { promise?: Promise<unknown>; observed: boolean }
const imperativeAttempts = new WeakMap<object, ImperativeAttempt[]>();
function queryAuthority(client: QueryClient, query: Query): QueryAuthority {
  let authority = queryAuthorities.get(query);
  if (!authority) {
    authority = { client, attempts: new WeakMap(), publications: [] };
    queryAuthorities.set(query, authority);
  }
  return authority;
}
function currentCertificate(authority: QueryAuthority, certificate: ReadCertificate) {
  return certificate.size > 0 && [...certificate].every(([identity, revision]) => pageReadAuthorityRevision(authority.client, identity) === revision);
}
/** Capture the attempt, not Query.promise at completion (which may then be a
 * recovery retryer). The app client applies this to EACH imperative caller,
 * because TanStack deduplicated callers receive the raw retryer promise and
 * bypass the initiating Query.fetch/cache-publication continuation. */
export function pageReadImperativeCompletion(client: QueryClient, query: Pick<Query, "meta" | "promise" | "queryHash"> & { state: { data: unknown } }, fromCache: boolean) {
  const cached = fromCache ? queryAuthorities.get(query)?.cached : undefined;
  const attempt: ImperativeAttempt | undefined = fromCache ? undefined : { promise: query.promise, observed: false };
  let stack = imperativeAttempts.get(query);
  if (attempt) {
    if (!stack) imperativeAttempts.set(query, stack = []);
    stack.push(attempt);
  }
  const identity = query.meta?.readIdentity;
  // Bootstrap may seed verified, account-scoped cache before a factory read.
  // Such seeds are admitted only before any refusal; they never renew a fence.
  const seeded = fromCache && !cached && typeof identity === "string" && pageReadAuthorityRevision(client, identity) === 0
    ? new Map([[identity, 0]]) : undefined;
  return {
    finish(promise?: Promise<unknown>) {
      if (!attempt || !stack) return;
      // Paused reads have not entered the factory yet. Joined/started reads
      // otherwise retain the promise captured on entry, even if reentrant
      // recovery has since replaced Query.promise.
      if (!attempt.observed) attempt.promise = promise;
      stack.splice(stack.indexOf(attempt), 1);
      if (!stack.length) imperativeAttempts.delete(query);
    },
    validate<T>(value: T): T {
      const authority = queryAuthorities.get(query);
      if (!authority && query.meta?.pageRead !== true) return value;
      if (client.getQueryCache().get(query.queryHash) !== query) throw new CancelledError({ revert: true });
      const certificate = attempt?.promise ? authority?.attempts.get(attempt.promise) : cached?.certificate ?? seeded;
      if (certificate && certificate.size > 0 && [...certificate].every(([address, revision]) => pageReadAuthorityRevision(client, address) === revision)) return value;
      if (authority?.cached && Object.is(query.state.data, authority.cached.value) && currentCertificate(authority, authority.cached.certificate)) {
        return query.state.data as T;
      }
      throw new CancelledError({ revert: true });
    },
  };
}
/** Query.setData calls structuralSharing BEFORE its success dispatch. Keep
 * this hook on the factory so remount/options updates cannot remove it.
 * Bind actual Query instances, not values or keys: one descriptor may be
 * reused across clients, and selected values may themselves be primitives.
 * The registry/hook is shared across fresh descriptors, including remounts
 * that replace options without starting a new read. Registrations are weak. */
const registered = new WeakSet<Query>();
const queries = new Set<WeakRef<Query>>();
const publicationBoundary = {
    observe(context: PageReadContext) {
      const query = context.queryKey && context.client?.getQueryCache().find({ queryKey: context.queryKey, exact: true });
      if (!query || !context.client) return;
      if (!registered.has(query)) { registered.add(query); queries.add(new WeakRef(query)); }
      const authority = queryAuthority(context.client, query);
      const promise = query.promise;
      const stack = imperativeAttempts.get(query);
      const imperative = stack?.[stack.length - 1];
      if (imperative) { imperative.promise = promise; imperative.observed = true; }
      if (!promise || authority.attempts.has(promise)) return;
      const certificate: ReadCertificate = new Map();
      authority.attempts.set(promise, certificate);
      // Installed before Query.fetch awaits retryer.start(). This reaction
      // records the OUTER factory's final result (including transformations)
      // before its queued setData. Tickets retain attempt provenance even for
      // identical primitive values; no markers/wrappers escape in payloads.
      void promise.then((value) => {
        authority.publications.push({ value, certificate: new Map(certificate), promise });
      }, () => {});
    },
    structuralSharing(this: object, previous: unknown, value: unknown): unknown {
      for (const reference of queries) {
        const query = reference.deref();
        if (!query) { queries.delete(reference); continue; }
        // Observer selectors also call structuralSharing, with their own
        // options object. Only the Query's cache publication needs a ticket.
        if (query.options !== this) continue;
        const authority = queryAuthorities.get(query);
        if (!authority) throw new SupersededAuthorityError();
        const index = authority.publications.findIndex((publication) => Object.is(publication.value, value));
        const manual = authority.manual && Object.is(authority.manual.value, value) ? authority.manual : undefined;
        const publication = manual || index < 0 ? undefined : authority.publications.splice(index, 1)[0];
        const certificate = manual?.certificate ?? publication?.certificate;
        if (!certificate || !currentCertificate(authority, certificate)) {
          // An obsolete fulfilled retryer must not replace a newer verified
          // recovery/withholding, or erase its current failure state.
          const safe = authority.cached && Object.is(query.state.data, authority.cached.value) && currentCertificate(authority, authority.cached.certificate);
          if (query.state.data !== undefined && !safe) {
            publishPageReadRefusal(query, query.state.error ?? new SupersededAuthorityError());
          } else if (publication?.promise === query.promise) {
            query.setState({ fetchStatus: "idle" });
          }
          // Core's public cancellation path returns current safe cache data
          // (or rejects if absent), skips success callbacks, and preserves a
          // newer retryer's state. Returning data here instead would let
          // fetchQuery resolve the obsolete raw result despite a safe cache.
          throw new CancelledError({ revert: true });
        }
        const shared = replaceEqualDeep(previous, value);
        authority.cached = { value: shared, certificate: new Map(certificate) };
        return shared;
      }
      return replaceEqualDeep(previous, value);
    },
};
export function pageReadPublicationBoundary() { return publicationBoundary; }
function publishVerifiedStored(client: QueryClient, query: Query, identity: string, revision: number, value: unknown) {
  const authority = queryAuthority(client, query);
  const previous = authority.manual;
  authority.manual = { value, certificate: new Map([[identity, revision]]) };
  try {
    query.setData(value, { manual: true });
    authority.cached = { value: query.state.data, certificate: authority.manual.certificate };
  }
  finally { authority.manual = previous; }
}
export function pageReadAuthorityRevision(client: QueryClient, identity: string) { return generations.get(client)?.get(identity) ?? 0; }
function publishPageReadRefusal(query: Query, error: Error) {
  // A previous key's synchronous notification may have started this retryer
  // after the resource-wide cancellation pass. Settle it before notifying.
  void query.cancel({ revert: false, silent: true });
  query.setState({ data: undefined, dataUpdatedAt: 0, error, status: "error", fetchStatus: "idle" });
}
function cancelPageReads(client: QueryClient, identity: string) {
  // TanStack settles retryers synchronously, including the initiating read.
  // Do this before publishing: removing the last observer otherwise cancels
  // with revert:true and restores the private pre-fetch snapshot. Silent
  // cancellation also prevents a later CancelledError from replacing the fence.
  return client.cancelQueries({ predicate: (q) => q.meta?.readIdentity === identity }, { revert: false, silent: true });
}
/** Interactive verified reads can discover the same resource's revocation.
 * Fence the indexed cache too; never remove an observed query to hide it. */
export async function refusePageRead(client: QueryClient, identity: string, error: Error) {
  let epochs = generations.get(client);
  if (!epochs) generations.set(client, epochs = new Map());
  epochs.set(identity, (epochs.get(identity) ?? 0) + 1);
  let publications = publishingRefusals.get(client);
  if (!publications) publishingRefusals.set(client, publications = new Map());
  const previous = publications.get(identity);
  publications.set(identity, error);
  let cancellation: Promise<void>;
  try {
    cancellation = cancelPageReads(client, identity);
    for (const query of client.getQueryCache().findAll({ predicate: (q) => q.meta?.readIdentity === identity })) {
      publishPageReadRefusal(query, error);
    }
  } finally {
    if (previous) publications.set(identity, previous);
    else publications.delete(identity);
  }
  await cancellation;
}
class SupersededAuthorityError extends Error {
  constructor() { super("This answer predates an access refusal. Retry to verify the current scope."); }
}
export function terminalPageReadError(error: unknown): boolean {
  return error instanceof SyntaxError || error instanceof StoredCertificateError || error instanceof SupersededAuthorityError ||
    error instanceof ApiError && [401, 403, 404].includes(error.status);
}

/** An access refusal fences the resource, including alternate factory keys.
 * Clearing the shared cache (rather than a hook-local boolean) also clears
 * payload-only observers. Cancelling peer retryers prevents late responses
 * from restoring authority, even when a network mock ignores AbortSignal. */
export async function authorizedPageRead<T>(context: PageReadContext, identity: string, read: () => Promise<T>, options: { publishStored?: boolean; withheld?: (value: T) => boolean; verified?: (value: T) => void } = {}): Promise<T> {
  const client = context.client;
  const publication = client && publishingRefusals.get(client)?.get(identity);
  if (client && publication) {
    // Query.fetch installs its retryer before invoking this function. Reject
    // reentrant reads synchronously, before token/transport work or another
    // fence notification can remove their last observer and revert old data.
    const query = context.queryKey && client.getQueryCache().find({ queryKey: context.queryKey, exact: true });
    if (query) publishPageReadRefusal(query, publication);
    throw new SupersededAuthorityError();
  }
  let epochs = client && generations.get(client);
  if (client && !epochs) generations.set(client, epochs = new Map());
  let epoch = epochs?.get(identity) ?? 0;
  const query = context.queryKey && client?.getQueryCache().find({ queryKey: context.queryKey, exact: true });
  const certificate = query?.promise && queryAuthorities.get(query)?.attempts.get(query.promise);
  certificate?.set(identity, epoch);
  try {
    const value = await read();
    if (context.signal.aborted) throw context.signal.reason ?? new DOMException("Aborted", "AbortError");
    if (epochs && epoch !== (epochs.get(identity) ?? 0)) throw new SupersededAuthorityError();
    if (client && epochs) {
      if (options.withheld?.(value)) {
        epochs.set(identity, ++epoch);
        certificate?.set(identity, epoch);
        await client.cancelQueries({ predicate: (q) => q.meta?.readIdentity === identity &&
          JSON.stringify(q.queryKey) !== JSON.stringify(context.queryKey) }, { revert: false });
      }
      if (epoch !== (epochs.get(identity) ?? 0)) throw new SupersededAuthorityError();
      if (options.publishStored) {
        const model = context.queryKey && client.getQueryCache().find({ queryKey: context.queryKey, exact: true })?.meta?.model;
        for (const query of client.getQueryCache().findAll({ predicate: (q) => q.meta?.readIdentity === identity && q.meta?.model === model })) {
          // A manual success also replaces TanStack's rollback snapshot before
          // notifying observers. A withheld read can then finish normally (and
          // arm owed polling), while last-observer cancellation can only revert
          // to this verified envelope, never its previous private payload.
          publishVerifiedStored(client, query, identity, epoch, value);
        }
      }
    }
    options.verified?.(value);
    return value;
  } catch (error) {
    if (context.signal.aborted) throw context.signal.reason ?? error;
    if (epochs && epoch !== (epochs.get(identity) ?? 0)) throw new SupersededAuthorityError();
    if (client && epochs && terminalPageReadError(error) && !(error instanceof SupersededAuthorityError)) {
      await refusePageRead(client, identity, error as Error);
    }
    throw error;
  }
}
