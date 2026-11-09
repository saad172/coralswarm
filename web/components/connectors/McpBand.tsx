"use client";

import { Card, CopyPill, SwarmStream } from "@/components/ui";
import { MCP_URL } from "@/lib/mcpUrl";

/* The MCP band above the connector grid (design: ConnectorsPage). Address
   only: the per-assistant setup steps are not here. A display title with one
   italic brand word, the address in a CopyPill, and the swarm stream (tools
   flowing into the ocean, answers flowing out). `/settings/connections` drops
   its MCP guide and links here (W4). */

export function McpBand() {
  return (
    <Card flush className="mb-[24px] overflow-hidden" aria-labelledby="mcp-band-title">
      <div className="relative h-[112px] border-b border-line-2 bg-bg-subtle sm:h-[150px]">
        <div className="absolute inset-0">
          <SwarmStream lanes={9} />
        </div>
        <span className="absolute bottom-[28px] left-[16px] sm:bottom-[11px] font-mono text-[10px] uppercase tracking-[0.13em] text-hue-teal">
          in · notes, decisions, sessions
        </span>
        <span className="absolute bottom-[11px] left-[16px] sm:left-auto sm:right-[16px] font-mono text-[10px] uppercase tracking-[0.13em] text-brand-2">
          out · cited answers
        </span>
      </div>
      <div className="grid grid-cols-1 items-start gap-x-[24px] gap-y-[16px] px-[20px] pb-[18px] pt-[20px] md:grid-cols-[minmax(0,1fr)_auto]">
        <div className="flex min-w-0 flex-col gap-[6px]">
          <h2 id="mcp-band-title" className="font-display text-[28px] font-normal leading-[1.15] text-ink">
            Use CoralSwarm <i className="text-brand">anywhere</i>
          </h2>
          <p className="text-[13.5px] leading-[1.5] text-ink-2">
            Your AI tools add context to your ocean and ask it questions. Tool not listed? Anything that speaks MCP can add to it.
          </p>
        </div>
        <CopyPill value={MCP_URL} ariaLabel={`Copy the MCP address ${MCP_URL}`} />
      </div>
      <div className="border-t border-line-2 bg-bg-subtle px-[20px] py-[11px] font-mono text-[11px] text-ink-3">
        paste it into any MCP client · no key — you sign in with your CoralSwarm account
      </div>
    </Card>
  );
}
