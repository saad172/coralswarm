"use client";
import { Suspense, useEffect, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { useQueryClient } from "@tanstack/react-query";
import { noteConnectorCallback } from "@/lib/connectorActionPolicy";
import { useApp } from "@/lib/providers";
import { refreshPageSection } from "@/lib/pageSectionRefresh";
import { invalidate, invalidationFor } from "@/lib/appQueries";
import type { ConnectorDetail } from "@/lib/connectorsApi";
import { connectorsApi } from "@/lib/connectorsApi";
import { isConnectorKey } from "@/components/settings/providerMeta";
import { useCriticalPageReadLease } from "@/lib/pageReadRegistration";
import { Button } from "@/components/ui";
function Return() {
  const params = useSearchParams();
  const router = useRouter();
  const client = useQueryClient();
  const { authReady, getAuthToken, queryScope } = useApp();
  const [error, setError] = useState(false);
  const [retry, setRetry] = useState(0);
  const provider = params.get("provider");
  const result = params.get("result");
  const rawInstallationId = params.get("installation_id");
  const installationId = provider === "github" && rawInstallationId && /^\d+$/.test(rawInstallationId) ? rawInstallationId : null;
  useCriticalPageReadLease("connector callback completion", !error);
  const destination = provider && typeof window !== "undefined" && sessionStorage.getItem(`connector-return:${provider}`) === "onboarding" ? "/onboarding/connectors" : "/connectors";
  useEffect(() => {
    if (!authReady) return;
    let cancelled = false;
    const controller = new AbortController();
    let deadline: number | null = null;
    let expiry: ReturnType<typeof setTimeout> | undefined;
    setError(false);
    (async () => {
      if (!provider || !isConnectorKey(provider) || result === "error") throw new Error("return");
      const token = getAuthToken();
      if (!token) throw new Error("auth");
      const key = `connector-pending:${provider}`;
      const pending = sessionStorage.getItem(key);
      if (!pending && result !== "ok") throw new Error("missing callback result");
      if (pending) { await connectorsApi.complete(token, provider, pending); sessionStorage.removeItem(key); }
      // eslint-disable-next-line no-restricted-syntax -- completes a provider callback action, then waits for its real worker acknowledgement; not an ordinary page mount read
      const connector = await refreshPageSection<ConnectorDetail>(token, "connector_detail", { provider }, controller.signal, { getToken: getAuthToken, onAcknowledged: (time) => { deadline = time; expiry = setTimeout(() => controller.abort(), Math.max(0, time - Date.now())); } });
      // A connect or reconnect ran (Home's Fix ends here): the connector list
      // and Home's attention read (has_source, the broken connector) are
      // stale, whether it landed or is still pending. Fired, never awaited:
      // the callback is noted against the acknowledgement's deadline first,
      // and a slow refetch of an on-screen list must not outlive that
      // deadline, whose expiry aborts the controller and skips the note.
      if (connector.status === "not_connected") { void invalidate(client, invalidationFor.connectorChanged()); throw new Error("pending"); }
      if (!cancelled && !controller.signal.aborted && deadline !== null) noteConnectorCallback(client, queryScope, provider, deadline);
      void invalidate(client, invalidationFor.connectorChanged());
      if (!cancelled) router.replace(`${destination}?setup=${encodeURIComponent(provider)}${installationId ? `&installation_id=${installationId}` : ""}`);
    })().catch(() => { if (!cancelled) setError(true); }).finally(() => { if (expiry) clearTimeout(expiry); });
    return () => { cancelled = true; if (expiry) clearTimeout(expiry); controller.abort(); };
  }, [authReady, getAuthToken, provider, result, retry, router, destination, installationId, client, queryScope]);
  return error ? <div role="alert">Sign-in could not be completed. <Button onClick={() => setRetry(retry + 1)}>Try again</Button> <Button onClick={() => router.replace(destination)}>Back to connectors</Button></div> : <p role="status">Completing sign-in…</p>;
}
export default function Page() { return <Suspense><Return /></Suspense>; }
