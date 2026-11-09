import { normalizedLibraryScope, validateLibraryRequest, type LibraryScope } from "./libraryMemoryApi";

/** Mirrors the 32-graph bound `validateLibraryRequest` and the backend's
 *  `LibraryScope` enforce. */
const MAX_CHAT_GRAPHS = 32;

/** The workspaces every chat asks across by default: all readable ones, in
 *  account order. An account with more than 32 keeps the first 32 silently;
 *  there is no picker to reach the rest. */
export function defaultChatGraphIds(readableGraphIds: string[]): string[] {
  return readableGraphIds.slice(0, MAX_CHAT_GRAPHS);
}
/** The default all-workspaces scope, with no other selectors. Null until
 *  any workspace is readable. */
export function defaultChatScope(readableGraphIds: string[]): LibraryScope | null {
  const graphIds = defaultChatGraphIds(readableGraphIds);
  return graphIds.length ? { graph_ids: graphIds, outcome_versions: "all_retained" } : null;
}

/** URLs contain selector identities only; saved server scopes are revalidated.
 *  A URL with no selector asks across every readable workspace (first 32). */
export function readChatScope(params: URLSearchParams, readableGraphIds: string[]): LibraryScope | null {
  const supported = new Set(["id", "q", "returnLibrary", "graph_id", "library_scope"]);
  if (Array.from(params.keys()).some((key) => !supported.has(key)))
    throw new Error("This older chat scope needs an explicit Library selection. Its filters were not removed.");
  for (const selector of ["graph_id", "library_scope"])
    if (params.getAll(selector).length > 1) throw new Error("This chat link repeats scope selectors. Choose an explicit Library selection.");
  const encoded = params.get("library_scope");
  if (params.has("library_scope")) {
    if (!encoded) throw new Error("This saved chat scope is empty. Choose an explicit Library selection.");
    if (encoded.length > 8192) throw new Error("This saved chat scope is too large.");
    const value: unknown = JSON.parse(encoded);
    if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("This chat scope is invalid.");
    const known = ["graph_ids", "outcome_versions", "collection", "service_id", "connection", "container", "content_kind", "source_item_id", "source_version_id", "node_id"];
    if (Object.keys(value).some((k) => !known.includes(k))) throw new Error("This chat link contains unsupported scope selectors.");
    const scope = value as LibraryScope;
    for (const [nested, fields] of [[scope.connection, ["kind", "id"]], [scope.container, ["kind", "id", "provider_namespace"]]] as const)
      if (nested && Object.keys(nested).some((field) => !fields.some((allowed) => allowed === field))) throw new Error("This chat link contains unsupported account or container selectors.");
    validateLibraryRequest({ scope });
    if (params.has("graph_id")) {
      const linkedGraph = params.get("graph_id") || "";
      validateLibraryRequest({ scope: { graph_ids: [linkedGraph] } });
      const graphs = [...new Set(scope.graph_ids.map((id) => id.toLowerCase()))];
      if (graphs.length !== 1 || graphs[0] !== linkedGraph.toLowerCase()) throw new Error("This chat link contains conflicting workspace selectors. Choose an explicit Library selection.");
    }
    return normalizedLibraryScope(scope);
  }
  // Preserve supported graph-only links, without an ambient scope fallback
  // when the URL declared an empty or unsupported selector.
  if (!params.has("graph_id")) {
    const scope = defaultChatScope(readableGraphIds);
    if (scope) validateLibraryRequest({ scope });
    return scope;
  }
  const requested = params.get("graph_id");
  if (!requested) throw new Error("This chat workspace selector is empty. Choose an explicit Library selection.");
  const scope: LibraryScope = { graph_ids: [requested], outcome_versions: "all_retained" };
  validateLibraryRequest({ scope });
  return scope;
}
export function scopedChatHref(scope: LibraryScope, returnLibrary?: string): string {
  validateLibraryRequest({ scope });
  const params = new URLSearchParams({ library_scope: JSON.stringify(normalizedLibraryScope(scope)) });
  if (returnLibrary && /^\/home(?:[/?]|$)/.test(returnLibrary) && returnLibrary.length <= 12000) params.set("returnLibrary", returnLibrary);
  return `/chats?${params}`;
}
const idSet = (ids: string[]) => new Set(ids.map((id) => id.toLowerCase()));
/** True when the scope's workspaces are not exactly the default set (order
 *  and case aside): a `?graph_id=` link, a `library_scope` link or saved chat
 *  over a subset. Never true before any workspace is readable, since there is
 *  no default to compare against yet. */
function isWorkspaceSubset(scope: LibraryScope, readableGraphIds: string[]): boolean {
  const defaults = idSet(defaultChatGraphIds(readableGraphIds));
  const chosen = idSet(scope.graph_ids);
  return defaults.size > 0 && (chosen.size !== defaults.size || [...chosen].some((id) => !defaults.has(id)));
}
/** Narrowed below the default: a subset of workspaces, any selector beyond
 *  graph_ids, or current-versions-only. An all-workspaces chat is not. */
export function isNarrowedChatScope(scope: LibraryScope, readableGraphIds: string[]): boolean {
  return isWorkspaceSubset(scope, readableGraphIds) || !!(scope.source_item_id || scope.source_version_id || scope.collection || scope.service_id ||
    scope.connection || scope.container || scope.content_kind || scope.node_id || scope.outcome_versions === "current");
}
const COLLECTION_LABEL: Record<NonNullable<LibraryScope["collection"]>, string> = {
  meetings: "meetings", agent_sessions: "agent sessions",
  // Read-only legacy value for saved chats; goes with the work-session data purge.
  work_sessions: "work sessions",
};
/** What a narrowed chat is limited to, for the composer's "Only: …" chip.
 *  Null for an all-workspaces chat. One workspace is named from the admitted
 *  `graphs`; URLs carry ids, not titles, so everything else names the kind of
 *  selection rather than the thing itself. */
export function narrowedScopeLabel(scope: LibraryScope, graphs: { id: string; name: string }[]): string | null {
  const readable = graphs.map((g) => g.id);
  if (!isNarrowedChatScope(scope, readable)) return null;
  const chosen = [...idSet(scope.graph_ids)];
  const named = chosen.length === 1 ? graphs.find((g) => g.id.toLowerCase() === chosen[0])?.name : undefined;
  const workspaces = isWorkspaceSubset(scope, readable) && (named || `${chosen.length} workspace${chosen.length === 1 ? "" : "s"}`);
  return [workspaces, scope.source_version_id ? "one capture version" : scope.source_item_id && "one source", scope.node_id && "one node",
    scope.collection && COLLECTION_LABEL[scope.collection], scope.content_kind, scope.service_id,
    scope.connection && "one connected account", scope.container && `one ${scope.container.kind}`,
    scope.outcome_versions === "current" && "current versions"].filter((v): v is string => !!v).join(" · ");
}
