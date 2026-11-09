"use client";

import { useState } from "react";
import { Eyebrow, Select, Button, Mono } from "@/components/ui";
import { NO_EDIT_MESSAGE, NO_EDIT_TITLE, writeErrorMessage } from "@/lib/editAccess";
import { api, type Reef } from "@/lib/api";

/* Merge a duplicate topic into this one (entity resolution) — curation
   functionality not modelled in the mock's right rail, kept below the hero
   like archive/web-legacy-ui/screens/reef.page.tsx did ("real, existing
   functionality kept... rather than dropped"). Offered only among topics of
   the same kind. */
export function TopicMerge({
  reef,
  reefs,
  token,
  onMerged,
  canEdit = true,
}: {
  reef: Reef;
  reefs: Reef[];
  token: string | null;
  onMerged: () => void;
  /** Server-derived (`ReefDetail.can_edit`). When false the merge would be
   *  refused (shown as not-found), so the controls are disabled with a reason. */
  canEdit?: boolean;
}) {
  const [pick, setPick] = useState("");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [done, setDone] = useState<string | null>(null);

  const options = reefs.filter((r) => r.node_id !== reef.node_id && r.type === reef.type);
  if (options.length === 0) return null;

  async function merge() {
    if (!token || !pick) return;
    if (!canEdit) {
      setErr(NO_EDIT_MESSAGE);
      return;
    }
    setBusy(true);
    setErr(null);
    setDone(null);
    try {
      const r = await api.reefs.merge(token, reef.node_id, pick);
      setDone(`Merged — ${r.corals_moved} item(s) moved here, duplicate removed.`);
      setPick("");
      onMerged();
    } catch (e) {
      setErr(writeErrorMessage(e, "Couldn't merge"));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div>
      <Eyebrow className="mb-[9px]">merge a duplicate</Eyebrow>
      <Mono className="mb-[10px] block leading-[1.6] text-ink-4">
        Same thing under another name? Merge it in — its items move here and the duplicate is removed.
      </Mono>
      <div className="flex items-center gap-[8px]">
        <Select
          className="max-w-[360px]"
          value={pick}
          onChange={(e) => setPick(e.target.value)}
          disabled={!canEdit}
          title={canEdit ? undefined : NO_EDIT_TITLE}
        >
          <option value="">merge another topic into this one…</option>
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
          onClick={() => void merge()}
        >
          {busy ? "merging…" : "Merge"}
        </Button>
      </div>
      {!canEdit && <p className="mt-[8px] font-mono text-[11px] text-ink-3">{NO_EDIT_TITLE}.</p>}
      {done && <p className="mt-[8px] font-mono text-[11px] text-hue-teal">{done}</p>}
      {err && <p className="mt-[8px] font-mono text-[11px] text-brand-2">{err}</p>}
    </div>
  );
}
