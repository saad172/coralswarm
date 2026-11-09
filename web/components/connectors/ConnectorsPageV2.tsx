"use client";

import { Suspense, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { useQuery } from "@tanstack/react-query";
import { useApp } from "@/lib/providers";
import { usePageSection } from "@/lib/pageSections";
import type { ConnectorDetail, ConnectorList } from "@/lib/connectorsApi";
import { SectionLiveness } from "@/components/PageSection";
import { ApiError } from "@/lib/api";
import { connectorQueryOptions, connectorsQueryOptions } from "@/lib/appQueries";
import { ErrorNote, Loading, PageHead } from "@/components/ui";
import { ConnectorCard } from "./ConnectorCard";
import { McpBand } from "./McpBand";
import { connectErrorMessage } from "./connectErrorModel";
import { ConnectWizard } from "./wizard/ConnectWizard";

/* The redesigned Connectors page: PageHead,
   the MCP band, then one grid of one card for every connector from
   `GET /v1/connectors`. Removed from the old page: the desktop tile, "other
   ways in", the Zoom tile, the standalone Gmail add-on and followed-threads
   sections, and the toolkit links. */

export function ConnectorGrid({ onboarding = false }: { onboarding?: boolean }) {
  const basePath = onboarding ? "/onboarding/connectors" : "/connectors";
  const { getAuthToken, authReady, queryScope } = useApp();
  const router = useRouter();
  const searchParams = useSearchParams();
  const connectError = connectErrorMessage(searchParams.get("error"));
  const actionErr = null;

  const query = usePageSection<ConnectorList>("connectors_list", {}, true, connectorsQueryOptions({ getAuthToken, scope: queryScope }).queryKey);

  const [selected, setSelected] = useState<string | null>(() => searchParams.get("setup"));
  const onConnect = (provider: string) => setSelected(provider);
  const setup = useQuery({ ...connectorQueryOptions({ getAuthToken, scope: queryScope }, selected ?? "slack"), enabled: authReady && !!selected });
  // The wizard owns its session once open. Each save withholds the detail read
  // until the worker recomputes it; unmounting then would restart setup from
  // resume_step and drop the member's progress. Closing clears it, because a
  // disabled query keeps its cached detail.
  const [opened, setOpened] = useState<ConnectorDetail | undefined>();
  const verified = selected ? setup.data : undefined;
  if (verified && verified !== opened) setOpened(verified);
  if (!selected && opened) setOpened(undefined);
  const active = selected && opened?.key === selected ? opened : undefined;

  const loadErr = query.error
    ? query.error instanceof ApiError
      ? query.error.message
      : "Failed to load connectors"
    : null;

  return (
    <>
      <div className="mb-3 flex justify-end"><SectionLiveness state={query} title="Connector activity" /></div>
      {selected && !active && !setup.error && <Loading what="loading connector setup" />}
      {selected && setup.error && <ErrorNote>Couldn’t load saved connector choices.</ErrorNote>}
      {active && <ConnectWizard key={active.key} connector={active} returnTo={onboarding ? "onboarding" : "wizard"} onClose={() => { setSelected(null); router.replace(basePath); }} />}
      {connectError && (
        <div role="status" className="mb-[18px] rounded-card border border-line px-[18px] py-[14px] text-[13.5px] text-ink">
          {connectError}
        </div>
      )}
      {actionErr && (
        <div className="mb-[16px]">
          <ErrorNote>{actionErr}</ErrorNote>
        </div>
      )}
      {loadErr && <ErrorNote>{loadErr}</ErrorNote>}
      {(query.isPending || (!query.error && query.data === undefined)) && <Loading what="loading connectors" />}
      {query.data !== undefined && (
        <div className="grid grid-cols-1 gap-[12px] md:grid-cols-3">
          {(query.data?.connectors ?? []).map((c) => (
            <ConnectorCard key={c.key} connector={c} recommended={onboarding && c.key === "slack" && c.status === "not_connected"} onConnect={(p) => void onConnect(p)} />
          ))}
        </div>
      )}
    </>
  );
}

export function ConnectorsPageV2() {
  return (
    <>
      <PageHead title="Connectors">
        Sign in to the tools you already use and choose exactly what CoralSwarm captures from each.
      </PageHead>
      <McpBand />
      {/* useSearchParams needs a Suspense boundary when output=export. */}
      <Suspense fallback={<Loading what="loading connectors" />}>
        <ConnectorGrid />
      </Suspense>
    </>
  );
}
