// W4 — the query-key hierarchy, declared once.
//
// Plan: docs/plans/request-reliability-and-scale.md §6.4.
//
// WHY THIS FILE EXISTS AT ALL. A read cache introduces a staleness class the
// current code cannot have, because today every screen refetches from scratch.
// That is the one way W4 can make correctness WORSE, and the original plan
// omitted it entirely — so this is designed up front rather than per screen.
//
// The rule: **a mutation invalidates a key PREFIX, never a specific key.** If a
// mutation had to enumerate every key it affects, it would miss the ones added
// after it was written — and the failure mode is silent. A rename that appears
// to do nothing is indistinguishable, to a user, from a rename that failed.
//
// Every key here is a tuple whose first elements narrow monotonically:
//
//   ['graph', graphId]                      ← invalidates EVERYTHING for an ocean
//   ['graph', graphId, 'reefs']             ← …just its reef lists
//   ['graph', graphId, 'reefs', 'all']
//   ['node',  nodeId]                       ← invalidates everything for one reef
//   ['node',  nodeId, 'atoms']
//
// so `invalidateQueries({ queryKey: keys.graph(id) })` reaches every descendant
// without naming them.

/** Everything scoped to one ocean. */
export const graphKey = (graphId: string) => ["graph", graphId] as const;

/** Everything scoped to one reef. */
export const nodeKey = (nodeId: string) => ["node", nodeId] as const;

export const keys = {
  /** Broadest ocean prefix — the invalidation target for any mutation that
   *  can change what a list view shows or what a facet counts. */
  graph: graphKey,
  node: nodeKey,

  reefsAll: (graphId: string) => [...graphKey(graphId), "reefs", "all"] as const,
  reefsPage: (graphId: string, page: number, pageSize: number, type?: string) =>
    [...graphKey(graphId), "reefs", "page", { page, pageSize, type: type ?? null }] as const,

  atomsAll: (graphId: string) => [...graphKey(graphId), "atoms", "all"] as const,

  /** W3's bulk edge drain. Under the same graph prefix as reefs and atoms
   *  because a reef mutation changes all three. */
  edges: (graphId: string) => [...graphKey(graphId), "edges"] as const,
  edgeFacets: (graphId: string) => [...graphKey(graphId), "edges", "facets"] as const,

  sources: (graphId: string) => [...graphKey(graphId), "sources"] as const,
  stats: (graphId: string) => [...graphKey(graphId), "stats"] as const,

  /** Per-reef corals — the detail-surface fetch W3 made on-demand. */
  nodeAtoms: (nodeId: string) => [...nodeKey(nodeId), "atoms"] as const,

  /** The caller's ocean list. Not under a graph prefix: it is the thing that
   *  tells you which graphs exist. */
  graphList: () => ["graphs"] as const,
} as const;

// What each mutation invalidates is declared once, in lib/appQueries.ts's
// `invalidationFor`, next to the app-resource prefixes it also has to reach.
