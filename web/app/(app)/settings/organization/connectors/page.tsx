"use client";

import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import { useApp } from "@/lib/providers";
import { ApiError } from "@/lib/api";
import { oauthConnectionsQueryOptions } from "@/lib/appQueries";
import { Empty, ErrorNote, Loading, Mono } from "@/components/ui";
import { OrgSettingsShell } from "@/components/org/OrgSettingsShell";
import { GITHUB_CONNECTOR_TILE, KNOWN_PROVIDERS, PROVIDER_META, visibleProviders } from "@/components/settings/providerMeta";
import { ProviderMark } from "@/components/settings/ProviderIcon";

/* Read-only. Connections are per-account (`api.oauth.listConnections`); there
   is no separate org-wide install to read or configure, and org connector
   policy is not part of this release, so nothing here is editable. This is the
   same data Settings → Connections reads. */
function OrgConnectorTiles() {
  const { authReady, getAuthToken, queryScope } = useApp();
  // Same cached entry Settings → Connections reads.
  const query = useQuery({ ...oauthConnectionsQueryOptions({ getAuthToken, scope: queryScope }), enabled: authReady });
  const connections = query.data ?? (query.isError ? [] : null);
  const err = query.isError && !query.data
    ? (query.error instanceof ApiError ? query.error.message : "Couldn't load connectors.")
    : null;

  if (connections === null) return <Loading what="connectors" />;
  if (err) return <ErrorNote>{err}</ErrorNote>;

  // `github` drops out while its rollout flag is off — same gating
  // `visibleProviders` applies on the personal Connectors page, so the two
  // surfaces can never disagree about whether it's visible.
  const connected = visibleProviders(KNOWN_PROVIDERS, GITHUB_CONNECTOR_TILE).filter((p) =>
    p !== "slack" && connections.some((c) => c.provider === p),
  );
  if (connected.length === 0) {
    return <Empty title="Nothing connected yet" hint="Connect a source from the Connectors page." />;
  }

  return (
    <div className="grid grid-cols-1 gap-[12px] sm:grid-cols-2">
      {connected.map((p) => {
        const meta = PROVIDER_META[p];
        const c = connections.find((x) => x.provider === p)!;
        const permCount = c.scope ? c.scope.split(/[,\s]+/).filter(Boolean).length : null;
        return (
          <div
            key={p}
            className="flex flex-col gap-[10px] rounded-card border px-[15px] py-[14px]"
            style={{ borderColor: "var(--ok)", background: "color-mix(in srgb, var(--ok) 4%, var(--bg))" }}
          >
            <div className="flex items-start gap-[11px]">
              <ProviderMark provider={p} letter={meta.letter} hue={meta.hue} box={36} />
              <span className="min-w-0 flex-1">
                <span className="block text-[15px] font-semibold text-ink">{meta.label}</span>
                <Mono className="mt-[2px] block text-ok">✓ connected</Mono>
              </span>
            </div>
            <p className="text-[12.5px] leading-[1.5] text-ink-2">{meta.description}</p>
            <div className="flex items-center gap-[10px] border-t border-line-2 pt-[10px] font-mono text-[10.5px] text-ink-4">
              <span>{permCount !== null ? `${permCount} permissions granted` : "connected"}</span>
            </div>
          </div>
        );
      })}
    </div>
  );
}

export default function SettingsOrganizationConnectorsPage() {
  return (
    <OrgSettingsShell
      title="Connected sources"
      lead="The sources connected to this workspace. This list is read-only. Each person connects and disconnects their own accounts from Connectors."
    >
      <p className="mb-[16px] text-[13px] text-ink-2">
        <Link href="/connectors" className="text-brand-2 underline">Go to Connectors →</Link>
      </p>
      <OrgConnectorTiles />
    </OrgSettingsShell>
  );
}
