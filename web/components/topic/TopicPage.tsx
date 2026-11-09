"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { useEvidenceAdmission } from "@/lib/useEvidenceAdmission";
import { useLibraryRead } from "@/lib/useLibraryRead";
import { usePageSection } from "@/lib/pageSections";
import { parseReefDigest, type ReefDigestView } from "@/lib/graphSurfaces";
import type { LibraryNodeSessions, LibraryNeighborhood } from "@/lib/libraryHistoryApi";
import { searchLibrary, type LibrarySearchRequest, type LibrarySearchResponse } from "@/lib/libraryMemoryApi";
import { librarySourceSupportLabel, nodeLabel } from "@/lib/topicLabels";
import { sourceEvidenceHref } from "@/lib/provenance";
import { Badge, Button, Card, Chip, Empty, ErrorNote, Loading, PageHead } from "@/components/ui";
import { DetailBreadcrumb } from "@/components/detail/DetailBreadcrumb";
import { TopicDigest } from "./TopicDigest";
import { TopicCaptureStatus } from "./TopicCaptureStatus";
import { TopicDisagreements } from "./TopicDisagreements";
import { TopicSessions } from "./TopicSessions";
import { TopicConnections } from "./TopicConnections";
import { OutcomeHistoryList } from "./TopicOutcomeHistory";

/* The topic (reef) page at `/topics?id=<node_id>`: a standalone page you land on
   from chat citations, Today's side panel, and the REEFS TOUCHED chips. There
   is no index of topics.

   It is the Library's node view, moved: the same digest, capture status,
   sourced statements, supporting sessions and connections, now keyed by the
   node alone. The node's workspace is proven first (`resolveP6NodeGraph`), then
   one `POST /v1/library/search` scoped to that node returns the node and its
   statements. Paging is local state (the Library kept it in the URL). */

const OutcomeList = OutcomeHistoryList;

interface TopicPayload {
  node: { node_id: string; graph_id: string } | null;
  edit_requires_admin: boolean;
  digest: unknown | null;
  sessions: LibraryNodeSessions | null;
  neighborhood: LibraryNeighborhood | null;
  search: LibrarySearchResponse | null;
}

export function TopicPage({ nodeId, graphId }: { nodeId: string; graphId?: string }) {
  const router = useRouter();
  const admission = useEvidenceAdmission();
  const getToken = () => {
    try {
      return admission.getToken();
    } catch {
      return null;
    }
  };
  const [after, setAfter] = useState<{ outcomes: string | null; sessions: string | null; connections: string | null; disagreements: string | null }>({
    outcomes: null,
    sessions: null,
    connections: null,
    disagreements: null,
  });
  const [showDisagreement, setShowDisagreement] = useState(false);
  const [unavailable, setUnavailable] = useState<string | null>(null);

  const section = usePageSection<TopicPayload>("topic_page", { node: nodeId }, admission.ready);
  const resolvedGraph = section.data?.node?.graph_id;
  const linkedGraphMismatch = !!(graphId && resolvedGraph && graphId.toLowerCase() !== resolvedGraph.toLowerCase());
  const request: LibrarySearchRequest | null = resolvedGraph && !linkedGraphMismatch ? {
    query: "", scope: { graph_ids: [resolvedGraph], node_id: nodeId, outcome_versions: "all_retained" },
    ...(after.outcomes ? { outcomes_after: after.outcomes } : {}),
  } : null;
  // The first page belongs to the certified snapshot; later pages keep their
  // interactive reader and its exact selectors/receipt checks.
  const later = useLibraryRead(JSON.stringify([admission.key, "topic", request]), admission.ready && !!request && !!after.outcomes, async (signal) => {
    const token = getToken();
    if (!token || !request) throw new Error("Account changed. Verify the current account and retry.");
    return searchLibrary(token, request, signal);
  });
  const read = { data: after.outcomes ? later.data : section.data?.search ?? undefined,
    loading: after.outcomes ? later.loading : section.isPending };
  const retry = () => { if (after.outcomes) later.refresh(); else void section.refresh(); };
  const node = linkedGraphMismatch ? undefined : read.data?.nodes.items.find((n) => n.node_id === nodeId);
  const error = section.error?.message ?? later.error ?? later.refreshError ?? unavailable ??
    (linkedGraphMismatch ? "This topic does not belong to the linked workspace." : null);
  let digest: ReefDigestView | undefined;
  try { digest = section.data?.digest === null || section.data?.digest === undefined ? undefined : parseReefDigest(section.data.digest); }
  catch { digest = undefined; }
  const openTopic = (id: string, graph: string) => router.push(`/topics?${new URLSearchParams({ id, graph_id: graph })}`);
  const openEvidence = (item: string, g: string, version: string, atom: string, passage: string) =>
    router.push(sourceEvidenceHref(item, g, version, atom, passage));

  return (
    <div className="min-w-0 space-y-6">
      <DetailBreadcrumb crumbs={[{ label: "Home", href: "/home" }, { label: node ? nodeLabel(node.title) : "Topic" }]} />
      {error && <ErrorNote action={<Button onClick={retry}>Retry topic</Button>}>{error}</ErrorNote>}
      {!error && read.loading && !read.data && <Loading what="verifying topic" />}
      {section.data !== undefined && !node && !read.loading && !error && (
        <Empty title="Topic unavailable" hint="This topic is not readable from your account, or it has no retained statements yet." />
      )}
      {node && request && resolvedGraph && !error && (
        <>
          <PageHead
            title={nodeLabel(node.title)}
            action={
              <Button
                variant="primary"
                onClick={() => router.push(`/chats?${new URLSearchParams({ graph_id: node.graph_id, q: `Tell me about ${nodeLabel(node.title)}` })}`)}
              >
                Ask about this
              </Button>
            }
          />
          <Card className="space-y-3">
            <div className="flex flex-wrap gap-2">
              <Chip>{node.entity_type || node.type}</Chip>
              {node.synthetic_demo && <Badge tone="warn">Contains synthetic demo evidence</Badge>}
              {node.unresolved_disagreement && <Badge tone="warn">Unresolved disagreement</Badge>}
            </div>
            <p className="text-sm leading-6 text-ink-2">
              {node.unresolved_disagreement
                ? "Supporting statements disagree. Neither side has been treated as resolved; a newer capture does not settle the disagreement."
                : "These are sourced captured statements."}
            </p>
            <TopicDigest identity={admission.key} getToken={getToken} nodeId={node.node_id} graphId={node.graph_id} supplied={{ view: digest, retry }} />
            {node.unresolved_disagreement && (
              <Button aria-expanded={showDisagreement} onClick={() => setShowDisagreement((v) => !v)}>
                {showDisagreement ? "Close disagreement" : "Inspect unresolved disagreement"}
              </Button>
            )}
            <TopicCaptureStatus capture={node.current_capture} />
            <dl className="grid gap-3 text-sm text-ink-2 sm:grid-cols-2">
              <div>
                <dt>Readable node context</dt>
                <dd>
                  {librarySourceSupportLabel(node.wider_context)} · {node.wider_context.semantic_atom_count} retained outcomes ·{" "}
                  {node.wider_context.unmapped_legacy_chunk_count} separate legacy chunks
                </dd>
              </div>
            </dl>
          </Card>

          <section aria-label="Sourced statements" className="space-y-3">
            <h2 className="font-display text-2xl font-normal text-ink">Facts, decisions, actions &amp; observations</h2>
            {read.data?.outcomes.items.length === 0 && (
              <Empty title="No readable statements yet" hint="Current captures may still await extraction. This does not establish no history." />
            )}
            <div className="space-y-3">
              {read.data && <OutcomeList outcomes={read.data.outcomes.items} onNode={openTopic} onEvidence={openEvidence} />}
            </div>
            {read.data?.outcomes.next_cursor && (
              <Button onClick={() => setAfter((a) => ({ ...a, outcomes: read.data?.outcomes.next_cursor ?? null }))}>Next statements →</Button>
            )}
            {after.outcomes && (
              <Button variant="ghost" onClick={() => setAfter((a) => ({ ...a, outcomes: null }))}>
                Return to first page
              </Button>
            )}
          </section>

          {node.unresolved_disagreement && showDisagreement && (
            <TopicDisagreements
              identity={admission.key}
              getToken={getToken}
              graphId={node.graph_id}
              nodeId={node.node_id}
              after={after.disagreements}
              onPage={(cursor) => setAfter((a) => ({ ...a, disagreements: cursor }))}
              onNode={openTopic}
              onEvidence={openEvidence}
              onUnavailable={setUnavailable}
            />
          )}
          <TopicSessions
            supplied={{ data: section.data?.sessions ?? null, retry }}
            identity={admission.key}
            getToken={getToken}
            scope={request.scope}
            nodeId={node.node_id}
            after={after.sessions}
            onPage={(cursor) => setAfter((a) => ({ ...a, sessions: cursor }))}
            onUnavailable={setUnavailable}
          />
          <TopicConnections
            supplied={{ data: section.data?.neighborhood ?? null, retry }}
            identity={admission.key}
            getToken={getToken}
            scope={request.scope}
            nodeId={node.node_id}
            after={after.connections}
            onPage={(cursor) => setAfter((a) => ({ ...a, connections: cursor }))}
            onNode={openTopic}
            onUnavailable={setUnavailable}
          />
        </>
      )}
    </div>
  );
}
