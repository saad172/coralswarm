"use client";

import type { ReactNode } from "react";
import { Badge, Button, Card, ErrorNote } from "./ui";
import { usePageSection, type PageModel, type SectionParams, type PageSectionState } from "@/lib/pageSections";

export interface PageSectionProps<P> {
  model: PageModel;
  params?: SectionParams;
  title: string;
  skeleton: ReactNode;
  empty?: ReactNode;
  isEmpty?: (data: P) => boolean;
  liveness?: "live" | "quiet";
  children: (data: P, state: PageSectionState<P>) => ReactNode;
}

/** A conservative display over independent sections, never a Stored certificate. */
export function sectionLiveness(states: PageSectionState<unknown>[]): PageSectionState<unknown> {
  const dates = states.map((state) => state.computedAt);
  return {
    data: undefined,
    computedAt: dates.length && dates.every((date) => date !== null) ? new Date(Math.min(...dates.map((date) => date!.getTime()))) : null,
    stale: states.some((state) => state.stale),
    isPending: states.some((state) => state.isPending),
    isRefreshing: states.some((state) => state.isRefreshing),
    error: states.find((state) => state.error)?.error ?? null,
    refresh: async () => { await Promise.all(states.map((state) => state.refresh())); },
  };
}

export function SectionLiveness({ state, title }: { state: PageSectionState<unknown>; title: string }) {
  const minutes = state.computedAt ? Math.max(0, Math.floor((Date.now() - state.computedAt.getTime()) / 60_000)) : null;
  return <div className="flex items-center gap-2">
    <Badge tone="ink">{state.isRefreshing ? "Refreshing…" : minutes === null ? "Awaiting update" : minutes < 1 ? "Updated just now" : `Updated ${minutes} min ago`}</Badge>
    <Button variant="ghost" size="sm" disabled={state.isPending} aria-label={`Refresh ${title}`} onClick={() => void state.refresh()}>Refresh</Button>
  </div>;
}

export function PageSection<P>({ model, params, title, skeleton, empty, isEmpty, liveness = "quiet", children }: PageSectionProps<P>) {
  const state = usePageSection<P>(model, params);
  return <Card aria-label={title} aria-busy={state.isPending}>
    <div className="mb-3 flex items-center justify-between gap-3">
      <h2 className="font-medium text-ink">{title}</h2>
      {liveness === "live" && <SectionLiveness title={title} state={state} />}
    </div>
    {state.error && <ErrorNote action={<Button size="sm" variant="ghost" onClick={() => void state.refresh()}>Retry {title}</Button>}>{state.error.message}</ErrorNote>}
    {state.isPending ? skeleton : state.data !== undefined ? (isEmpty?.(state.data) ? empty : children(state.data, state)) : null}
  </Card>;
}
