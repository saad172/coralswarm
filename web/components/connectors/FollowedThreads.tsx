"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { useAuth } from "@clerk/nextjs";
import { useInfiniteQuery, useQueryClient } from "@tanstack/react-query";
import { useApp } from "@/lib/providers";
import { api, type MailboxProvider, type OAuthConnection, type WatchedThread, type WatchedThreadsResponse } from "@/lib/api";
import { isPersonalOrgKnownForActiveOrg } from "@/lib/orgAdmin";
import { tokenIdentityKey } from "@/lib/engine/engineSession";
import { vaultIdentityKey } from "@/lib/engine/vaultNamespace";
import { Button, Card, ErrorNote, Loading, Mono } from "@/components/ui";
import { watchedThreadsQueryOptions } from "@/lib/appQueries";
import { appendWatchedThreads, followedThreadsError } from "./followedThreadsModel";

function MailboxThreads({ provider, label, connected, getAuthToken, scope, initialPage }: {
  provider: MailboxProvider;
  label: string;
  connected: boolean;
  getAuthToken: () => string | null;
  /** The full mailbox identity (account, org, session, user) — the cache key,
   *  so an outgoing account's rows and late answers can never surface here. */
  scope: string;
  initialPage?: WatchedThreadsResponse;
}) {
  const qc = useQueryClient();
  const options = { ...watchedThreadsQueryOptions({ getAuthToken, scope }, provider), ...(initialPage ? { initialData: { pages: [initialPage], pageParams: [undefined] }, staleTime: Infinity, enabled: false } : {}) };
  const query = useInfiniteQuery(options);
  const threads = useMemo(
    () => (query.data?.pages ?? []).reduce<WatchedThread[]>((rows, page) => appendWatchedThreads(rows, page.threads), []),
    [query.data],
  );
  const loaded = query.data !== undefined;
  const cursor = query.data?.pages.at(-1)?.next_cursor ?? null;
  const loading = query.isFetching;
  const [stopError, setStopError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [stopping, setStopping] = useState<string | null>(null);
  // One action at a time per mailbox: a stop and a page read never overlap,
  // and a double click on Stop sends one PUT.
  const busy = useRef(false);
  const mounted = useRef(true);
  useEffect(() => () => { mounted.current = false; }, []);

  const loadError = query.error ? followedThreadsError(query.error, "load") : null;
  const error = stopError ?? loadError;
  const errorAction: "load" | "stop" = stopError ? "stop" : "load";

  function load(next?: string) {
    if (busy.current || stopping !== null) return;
    setStopError(null);
    if (!getAuthToken()) return;
    void (next !== undefined ? query.fetchNextPage({ cancelRefetch: false }) : query.refetch({ cancelRefetch: false }));
  }

  async function stopFollowing(threadId: string) {
    if (busy.current || loading) return;
    const token = getAuthToken();
    if (!token) {
      setStopError("Your session isn't ready yet. Sign in again and try stopping following.");
      return;
    }
    busy.current = true;
    setStopping(threadId);
    setStopError(null);
    setNotice(null);
    try {
      const response = await api.connectors.setThreadFollow(token, provider, threadId, false);
      if (!mounted.current) return;
      if (!response || typeof response.following !== "boolean") throw new Error("Invalid follow response");
      qc.setQueryData(options.queryKey, (data) => data && {
        ...data,
        pages: data.pages.map((page) => ({
          ...page,
          threads: page.threads.map((row) => row.thread_id === threadId ? { ...row, following: response.following } : row),
        })),
      });
      setNotice(response.following ? "This thread is still followed. Try stopping following again." : "Stopped following");
    } catch (e) {
      if (mounted.current) setStopError(followedThreadsError(e, "stop"));
    } finally {
      busy.current = false;
      if (mounted.current) setStopping(null);
    }
  }

  const followed = threads.filter((row) => row.following);
  return (
    <section aria-label={`${label} followed threads`} className="flex flex-col gap-3">
      <div className="flex items-center justify-between gap-3">
        <span className="text-[13.5px] font-medium text-ink">{label}</span>
        {connected && <Mono className="text-ok">connected</Mono>}
      </div>
      {notice && <p role="status" className="text-[12.5px] text-ink-2">{notice}</p>}
      {error && <ErrorNote action={errorAction === "load" && stopping === null && !loading
        ? <Button size="sm" onClick={() => void load(loaded && cursor !== null ? cursor : undefined)}>Retry</Button>
        : undefined}>{error}</ErrorNote>}
      {loading && !loaded ? <Loading what={`loading ${label} threads`} className="py-6" /> : (
        <>
          {followed.length > 0 ? (
            <ul className="overflow-hidden rounded-control border border-line-2">
              {followed.map((row) => (
                <li key={row.thread_id} className="flex items-center gap-3 border-b border-line-3 px-3 py-3 last:border-b-0">
                  <span className="min-w-0 flex-1">
                    <span className="block truncate text-[13px] text-ink" title={row.title || row.thread_id}>
                      {row.title || `Thread ${row.thread_id}`}
                    </span>
                    <span className="mt-1 block font-mono text-[10px] text-ink-3">
                      saved {new Date(row.saved_at).toLocaleDateString()}
                    </span>
                  </span>
                  <Button size="sm" disabled={loading || stopping !== null} aria-label={`Stop following ${row.title || row.thread_id}`}
                    onClick={() => void stopFollowing(row.thread_id)}>
                    {stopping === row.thread_id ? "Stopping…" : "Stop following"}
                  </Button>
                </li>
              ))}
            </ul>
          ) : loaded && !error && (
            <p className="rounded-control border border-dashed border-line bg-bg-2 px-3 py-3 text-[12.5px] leading-[1.55] text-ink-2">
              {cursor !== null
                ? "No followed threads found yet. Load older saved threads to keep looking."
                : `No ${label} threads are followed. Save an email with the ${label === "Gmail" ? "Gmail add-on" : "Outlook add-in"} and “Keep saving new replies” on to follow its thread.`}
            </p>
          )}
          {cursor !== null && !(error && errorAction === "load") && <Button size="sm" className="self-start" disabled={loading || stopping !== null}
            onClick={() => void load(cursor)}>{loading ? "Loading…" : `Load more ${label} threads`}</Button>}
        </>
      )}
    </section>
  );
}

/** Personal orgs resolve to the personal plane too. Require matching boot
 * metadata before mounting children; the identity key remounts each mailbox
 * on any account/session/org change, hiding outgoing rows during render and
 * discarding its late GET/PUT answers after cleanup. */
export function FollowedThreads({ connections, mailboxProvider, initialPage, computedAt }: { connections: OAuthConnection[]; mailboxProvider?: MailboxProvider; initialPage?: WatchedThreadsResponse; computedAt?: string }) {
  const { getAuthToken, authReady, ready, accountId, isPersonalOrg, resolvedOrgId } = useApp();
  const { isLoaded, orgId, sessionId, userId } = useAuth();
  const known = isPersonalOrgKnownForActiveOrg({ ready, resolvedOrgId, activeOrgId: orgId ?? null });
  if (!isLoaded || !known || !authReady || !accountId || !sessionId || !userId || (orgId && !isPersonalOrg)) return null;
  // Clerk identity updates before the provider's asynchronous bearer refresh.
  // Do not mount a new account's list with the outgoing identity's token.
  if (tokenIdentityKey(getAuthToken()) !== vaultIdentityKey({ isLoaded, userId, orgId })) return null;
  const identity = [accountId, orgId || "personal", sessionId || "", userId || "", computedAt ?? ""].join("|");
  return (
    <>
      <Mono className="mb-[10px] tracking-[0.13em]">followed threads</Mono>
      <Card className="mb-[22px] flex flex-col gap-4">
        <div>
          <span className="block text-[15px] font-semibold text-ink">Followed threads</span>
          <p className="mt-1 text-[13px] leading-[1.5] text-ink-2">
            New replies to these threads are saved to your workspace. Stop following to leave future replies in your mailbox.
          </p>
        </div>
        {([['google', 'Gmail', 'google_gmail'], ['microsoft', 'Outlook', 'microsoft']] as const).filter(([provider]) => !mailboxProvider || provider === mailboxProvider).map(([provider, label, connectionKey]) => (
          <MailboxThreads key={`${identity}|${provider}`} provider={provider} label={label} scope={identity}
            initialPage={initialPage} connected={connections.some((connection) => connection.provider === connectionKey)} getAuthToken={getAuthToken} />
        ))}
      </Card>
    </>
  );
}
