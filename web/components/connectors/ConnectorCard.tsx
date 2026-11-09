"use client";

import Link from "next/link";
import { Badge, Button, Card, Meter, Mono, Verdict, buttonClasses } from "@/components/ui";
import { ProviderMark } from "@/components/settings/ProviderIcon";
import {
  CONNECTOR_META,
  connectorBlurb,
  isConnectorKey,
  type ConnectorMeta,
} from "@/components/settings/providerMeta";
import type { ConnectorSummary } from "@/lib/connectorsApi";
import {
  freshnessLine,
  importProgress,
  primaryAction,
  problemLine,
  statusView,
} from "./statusModel";

/* One connector card, every connector, every status (design: ConnectorCard
   variant="tile"). Same look for Slack and for a Beta toolkit; Beta is a tag,
   nothing more.

   Status comes from `connector.status` and decides everything else: the pill
   (Verdict: the word and glyph carry the meaning, tone only tints), the line
   under it (what it takes / freshness / the problem sentence), the Meter while
   importing, and the one primary action.

   Actions are callbacks. Connect, Continue setup and Reconnect call
   `onConnect(key)` (the wizard owns what happens next, W2); Manage is a link
   to the detail page. */

const FALLBACK_META: ConnectorMeta = { label: "", hue: "coral", letter: "?", captures: "" };

export interface ConnectorCardProps {
  connector: ConnectorSummary;
  onConnect: (provider: string) => void;
  /** Onboarding's "start here": ink border, a tag, and the brand button. */
  recommended?: boolean;
  /** The clock for "last captured 3 min ago". Tests pin it. */
  now?: Date;
}

export function ConnectorCard({ connector, onConnect, recommended = false, now }: ConnectorCardProps) {
  const meta = isConnectorKey(connector.key) ? CONNECTOR_META[connector.key] : { ...FALLBACK_META, label: connector.name, letter: connector.name.slice(0, 1).toUpperCase() || "?" };
  const status = statusView(connector.status);
  const action = primaryAction(connector.status, recommended);
  const problem = problemLine(connector);
  const fresh = problem ? null : freshnessLine(connector, now ?? new Date());
  const importing = importProgress(connector);
  const blurb = connector.status === "not_connected" ? connectorBlurb(meta) : null;
  const name = connector.name;

  return (
    <Card
      emphasis={recommended}
      className="flex h-full flex-col gap-[12px]"
      data-connector={connector.key}
      data-status={connector.status}
    >
      <div className="flex items-center gap-[12px]">
        <ProviderMark provider={connector.key} letter={meta.letter} hue={meta.hue} box={38} />
        <div className="flex min-w-0 flex-1 flex-col items-start gap-[4px]">
          <div className="flex items-center gap-[7px]">
            <span className="text-[15px] font-semibold text-ink">{name}</span>
            {connector.beta && <Badge quiet className="px-[6px] py-[1px] text-[9.5px] uppercase tracking-[0.06em]">Beta</Badge>}
            {recommended && (
              <Badge tone="brand" className="px-[6px] py-[1px] text-[9.5px] uppercase tracking-[0.06em]">
                Start here
              </Badge>
            )}
          </div>
          <Verdict tone={status.tone} glyph={status.glyph}>
            {status.label}
          </Verdict>
        </div>
      </div>

      <div className="flex flex-1 flex-col gap-[8px]">
        {problem && <p className="text-[12.5px] leading-[1.5] text-ink">{problem}</p>}
        {fresh && <Mono className="text-[11px] leading-[1.5] text-ink-3">{fresh}</Mono>}
        {blurb && <p className="text-[12.5px] leading-[1.5] text-ink-3">{blurb}</p>}
        {importing && (
          <Meter
            value={importing.percent ?? 0}
            indeterminate={importing.percent === null}
            tone="var(--ok)"
            ariaLabel={`Importing ${name} history`}
          />
        )}
      </div>

      <div className="flex justify-end">
        {action.kind === "manage" ? (
          <Link
            href={`/connectors/${encodeURIComponent(connector.key)}`}
            className={buttonClasses({ variant: action.variant, size: "sm" })}
            aria-label={`${action.label} ${name}`}
          >
            {action.label}
          </Link>
        ) : (
          <Button
            variant={action.variant}
            size="sm"
            onClick={() => onConnect(connector.key)}
            aria-label={`${action.label} ${name}`}
          >
            {action.label}
          </Button>
        )}
      </div>
    </Card>
  );
}
