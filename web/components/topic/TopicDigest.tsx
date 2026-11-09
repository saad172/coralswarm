"use client";

import Link from "next/link";
import { digestAttributionLine, type ReefDigestView } from "@/lib/graphSurfaces";
import { readReefDigest } from "@/lib/reefDigestApi";
import { evidenceHref } from "@/lib/provenance";
import { useLibraryRead } from "@/lib/useLibraryRead";
import { Button, Eyebrow, Skeleton } from "@/components/ui";

/** The reef's attributed digest (#1357), read through `GET /v1/nodes/{id}/digest`.
 *  The server serves it only when this reader may see every coral it was
 *  generated from, so whatever renders here never exceeds the reader's access.
 *  Withheld digests say only "unavailable", never why. */
export function TopicDigest({ identity, getToken, nodeId, graphId, narrowed = false, supplied }: {
  supplied?: { view?: ReefDigestView; retry: () => void };
  identity: string;
  getToken: () => string | null;
  nodeId: string;
  graphId: string;
  /** The Library view is scoped to one contribution; the digest covers the whole reef. */
  narrowed?: boolean;
}) {
  const read = useLibraryRead<ReefDigestView>(JSON.stringify([identity, "digest", nodeId]), !!getToken() && !supplied, async (signal) => {
    const token = getToken();
    if (!token) throw new Error("Verify the current Library account.");
    return readReefDigest(token, nodeId, signal);
  });
  return <ReefDigestPanel view={supplied ? supplied.view : read.data} loading={!supplied && read.loading} error={supplied ? supplied.view ? undefined : "Summary unavailable." : read.error ?? read.refreshError} onRetry={supplied?.retry ?? read.refresh} graphId={graphId} narrowed={narrowed} />;
}

/** Presentation only, so states can be rendered and tested without a network. */
export function ReefDigestPanel({ view, loading, error, onRetry, graphId, narrowed = false }: {
  view?: ReefDigestView;
  loading?: boolean;
  error?: string;
  onRetry?: () => void;
  graphId: string;
  narrowed?: boolean;
}) {
  return (
    <section aria-label="Topic summary" aria-busy={loading || undefined} className="space-y-2 rounded-control border border-line-2 bg-bg-subtle p-4">
      <Eyebrow as="h3">Topic summary</Eyebrow>
      {loading && (
        <div className="space-y-2" role="status" aria-label="Reading topic summary">
          <Skeleton w="92%" />
          <Skeleton w="78%" />
          <Skeleton w="54%" />
        </div>
      )}
      {error && (
        <div className="flex flex-wrap items-center gap-3 text-sm text-ink-2">
          <p role="alert" className="min-w-0 flex-1">{error}</p>
          {onRetry && <Button size="sm" variant="ghost" onClick={onRetry}>Retry summary</Button>}
        </div>
      )}
      {view?.state === "unavailable" && (
        <p className="text-sm leading-6 text-ink-2">
          Summary unavailable. {view.regenerating ? "A fresh summary is being prepared from the items you can read; check back shortly." : "The sourced statements below are the record."}
        </p>
      )}
      {view?.state === "empty" && <p className="text-sm leading-6 text-ink-3">No readable items to summarize yet.</p>}
      {view?.state === "served" && <ServedDigest view={view} graphId={graphId} narrowed={narrowed} />}
    </section>
  );
}

function ServedDigest({ view, graphId, narrowed }: { view: Extract<ReefDigestView, { state: "served" }>; graphId: string; narrowed: boolean }) {
  const attribution = digestAttributionLine(view.attribution);
  return (
    <>
      {view.summary ? (
        <p className="whitespace-pre-wrap break-words text-[15px] leading-7 text-ink">{view.summary}</p>
      ) : (
        <p className="text-sm leading-6 text-ink-2">No overall summary; per-item excerpts are below.</p>
      )}
      <p className="font-mono text-[10.5px] text-ink-3">
        {attribution ? <>Generated {attribution}</> : "Generated summary"}
        {narrowed && " · covers the whole topic, not only this selection"}
      </p>
      {view.excerpts.length > 0 && (
        <details className="pt-1">
          <summary className="cursor-pointer rounded-row text-sm text-brand-2 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand">
            Item excerpts · {view.excerpts.length}
          </summary>
          <ul className="mt-3 space-y-3">
            {view.excerpts.map((e, i) => (
              <li key={`${e.atom_id}:${i}`} className="border-l-2 border-line pl-3">
                <p className="whitespace-pre-wrap break-words text-sm leading-6 text-ink">{e.excerpt}</p>
                {e.contribution && <p className="mt-1 text-xs leading-5 text-ink-2">{e.contribution}</p>}
                <Link href={evidenceHref(e.atom_id, graphId)} className="mt-1 inline-block rounded-row text-xs text-brand-2 underline underline-offset-4 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand">
                  Open item {i + 1}
                </Link>
              </li>
            ))}
          </ul>
        </details>
      )}
    </>
  );
}
