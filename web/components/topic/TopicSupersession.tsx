"use client";

import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Eyebrow, Select, Button } from "@/components/ui";
import { NO_EDIT_MESSAGE, NO_EDIT_TITLE, writeErrorMessage } from "@/lib/editAccess";
import { api, type Reef, type SupersessionEdge } from "@/lib/api";
import { appInvalidation, supersessionsQueryOptions } from "@/lib/appQueries";
import { useApp } from "@/lib/providers";

const NO_EDGES: SupersessionEdge[] = [];

/* Supersession edges (curation): record that this topic replaces an older one
   (or is replaced by a newer one). "This supersedes X" posts to X with
   superseded_by = this topic — ported from archive's SupersessionControl.

   `canEdit` (fix/topics-derive-ocean's can_edit follow-up): `POST
   /v1/nodes/{id}/supersede` and `DELETE /v1/nodes/{id}/supersede/{other}`
   authorize with the SAME account-ownership predicate
   (`node_owned_by_account_scoped`) as `patch_node` — not the reachability
   check `GET` uses — so a promoted team reef this component can now be
   opened for (the ocean-derivation fix) can 404 ("node not found" /
   "superseder node not found") on every write here even though the read
   above succeeded. Same server-derived gate as `TopicHeader`'s Edit button:
   disabled, not hidden, with a tooltip; the write handlers refuse
   independently too, not relying on the disabled attribute alone.

   KNOWN LIMIT, not fixed here: the backend also checks ownership of the
   OTHER reef in the pair (`superseded_by`/`other_id`). `canEdit` only
   speaks to THIS reef. In practice `options` below is always drawn from
   `reefs`, which the caller sources from THIS reef's own ocean
   (`api.reefs.listAll(reef.graph_id)`), so every candidate shares this
   reef's `graph_id` — and ownership is account-scoped (checked via the
   reef's graph's `account_id`, not per-reef), so a same-ocean pick can't
   actually disagree with `canEdit` through this UI today. The endpoint
   itself has no such guarantee for a different caller (a direct API call,
   an MCP tool) passing an arbitrary cross-ocean id — that failure mode
   predates this PR and isn't attempted here. */
export function TopicSupersession({
  reef,
  reefs,
  token,
  canEdit = true,
}: {
  reef: Reef;
  reefs: Reef[];
  token: string | null;
  /** Server-derived (`ReefDetail.can_edit`) — see the file header. */
  canEdit?: boolean;
}) {
  const { authReady, getAuthToken, queryScope } = useApp();
  const qc = useQueryClient();
  // The bearer is read inside the query, never depended on: it rotates about
  // once a minute and keyed on it this re-read the links every time.
  const query = useQuery({
    ...supersessionsQueryOptions({ getAuthToken, scope: queryScope }, reef.node_id),
    enabled: authReady,
  });
  const supersedes = query.data?.as_superseder ?? NO_EDGES;
  const supersededBy = query.data?.as_superseded ?? NO_EDGES;
  // A write changes both ends of the pair, so every topic's links go stale.
  const refreshLinks = () => qc.invalidateQueries({ queryKey: appInvalidation.supersessions() });
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [pick, setPick] = useState("");

  const linked = new Set([
    reef.node_id,
    ...supersedes.map((e) => e.superseded_id),
    ...supersededBy.map((e) => e.superseder_id),
  ]);
  const options = reefs.filter((r) => !linked.has(r.node_id));

  async function markSupersedes() {
    if (!token || !pick) return;
    if (!canEdit) {
      // Fail-safe, not merely a UI nicety — see the file header. The
      // controls below already disable on `!canEdit`, but this refuses
      // independently rather than trusting the DOM attribute alone.
      setErr(NO_EDIT_MESSAGE);
      return;
    }
    setBusy(true);
    setErr(null);
    try {
      await api.reefs.supersede(token, pick, reef.node_id);
      setPick("");
      await refreshLinks();
    } catch (e) {
      setErr(writeErrorMessage(e, "Couldn't link"));
    } finally {
      setBusy(false);
    }
  }

  async function remove(otherId: string) {
    if (!token) return;
    if (!canEdit) {
      setErr(NO_EDIT_MESSAGE);
      return;
    }
    setBusy(true);
    setErr(null);
    try {
      await api.reefs.unsupersede(token, reef.node_id, otherId);
      await refreshLinks();
    } catch (e) {
      setErr(writeErrorMessage(e, "Couldn't remove"));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div>
      <Eyebrow className="mb-[9px]">supersession</Eyebrow>
      <div className="rounded-control border border-line bg-bg p-[11px]">
        {supersededBy.length === 0 && supersedes.length === 0 ? (
          <p className="mb-[8px] text-[12.5px] text-ink-3">No supersession links yet.</p>
        ) : (
          <div className="mb-[8px] flex flex-col gap-[6px]">
            {supersededBy.map((e) => (
              <div key={e.superseder_id} className="flex items-center justify-between gap-[8px] text-[12.5px]">
                <span className="text-ink-2">
                  superseded by <span className="text-ink">{e.superseder_title}</span>
                </span>
                <button
                  type="button"
                  onClick={() => void remove(e.superseder_id)}
                  disabled={busy || !canEdit}
                  title={canEdit ? undefined : NO_EDIT_TITLE}
                  className="font-mono text-[10px] text-ink-3 transition-colors hover:text-brand-2"
                >
                  ✕
                </button>
              </div>
            ))}
            {supersedes.map((e) => (
              <div key={e.superseded_id} className="flex items-center justify-between gap-[8px] text-[12.5px]">
                <span className="text-ink-2">
                  supersedes <span className="text-ink">{e.superseded_title}</span>
                </span>
                <button
                  type="button"
                  onClick={() => void remove(e.superseded_id)}
                  disabled={busy || !canEdit}
                  title={canEdit ? undefined : NO_EDIT_TITLE}
                  className="font-mono text-[10px] text-ink-3 transition-colors hover:text-brand-2"
                >
                  ✕
                </button>
              </div>
            ))}
          </div>
        )}
        {options.length > 0 && (
          <div className="flex items-center gap-[8px]">
            <Select
              className="flex-1"
              value={pick}
              onChange={(e) => setPick(e.target.value)}
              disabled={!canEdit}
              title={canEdit ? undefined : NO_EDIT_TITLE}
            >
              <option value="">mark this as superseding…</option>
              {options.map((r) => (
                <option key={r.node_id} value={r.node_id}>
                  {r.title}
                </option>
              ))}
            </Select>
            <Button
              size="sm"
              disabled={busy || !pick || !canEdit}
              title={canEdit ? undefined : NO_EDIT_TITLE}
              onClick={() => void markSupersedes()}
            >
              Mark
            </Button>
          </div>
        )}
        {err && <p className="mt-[8px] font-mono text-[11px] text-brand-2">{err}</p>}
      </div>
    </div>
  );
}
