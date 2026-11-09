import { StoredCertificateError, typedStored, type Stored } from "../pageSectionWire";

export interface OceanComponent<P = unknown> {
  account_id: string;
  anchor_graph_id: string | null;
  refresh_path: string;
  stored: Stored<P>;
}
export interface OceansIndex<P = unknown> { components: OceanComponent<P>[] }

/** Fan-in has no outer certificate. Every component keeps its own identity,
 * payload and owed refresh; no browser per-account discovery is necessary. */
export function parseOceansIndex<P = unknown>(body: unknown): OceansIndex<P> {
  const value = body as OceansIndex<P>;
  const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  if (!value || !Array.isArray(value.components) || value.components.length > 501) throw new StoredCertificateError("Could not verify workspaces.");
  const identities = new Set<string>();
  for (const component of value.components) {
    if (!component || typeof component !== "object") throw new StoredCertificateError("Could not verify workspace scope.");
    const identity = component.account_id;
    if (typeof component.account_id !== "string" || !uuid.test(component.account_id) || (component.anchor_graph_id !== null && (typeof component.anchor_graph_id !== "string" || !uuid.test(component.anchor_graph_id))) ||
        identities.has(identity) || typeof component.refresh_path !== "string") throw new StoredCertificateError("Could not verify workspace scope.");
    const expectedRefresh = `/v1/load/oceans/component/refresh${component.anchor_graph_id ? `?graph=${component.anchor_graph_id}` : ""}`;
    if (component.refresh_path !== expectedRefresh) throw new StoredCertificateError("Could not verify workspace refresh identity.");
    identities.add(identity);
    typedStored<P>(component.stored, "oceans_index");
    const payload = component.stored.payload as { oceans?: unknown; members?: unknown; team_members?: unknown; archived?: unknown } | null;
    if (component.stored.computed_at !== null && (!Array.isArray(payload?.oceans) ||
        !Array.isArray(payload?.members) || !payload?.team_members || typeof payload.team_members !== "object" ||
        (payload.archived !== null && !Array.isArray(payload.archived)))) throw new StoredCertificateError("Could not verify workspace data.");
    if (component.stored.computed_at !== null) for (const row of payload?.oceans as unknown[]) {
      if (!row || typeof row !== "object") throw new StoredCertificateError("Could not verify workspace counts.");
      const { graph, counts } = row as { graph?: { id?: unknown; name?: unknown }; counts?: Record<string, unknown> };
      if (!graph || typeof graph.id !== "string" || !uuid.test(graph.id) || typeof graph.name !== "string" || !counts ||
          !["reef_total", "atom_count", "source_count"].every((key) => Number.isSafeInteger(counts[key]) && Number(counts[key]) >= 0) ||
          !counts.reef_counts || typeof counts.reef_counts !== "object") throw new StoredCertificateError("Could not verify workspace counts.");
    }
  }
  return value;
}
