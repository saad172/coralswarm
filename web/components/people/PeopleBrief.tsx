"use client";

import { useState } from "react";
import { Eyebrow, Select, Button, SearchInput, ErrorNote, Mono } from "@/components/ui";
import { IconSearch } from "@/components/icons";
import { api, ApiError, type Member } from "@/lib/api";
import { useApp } from "@/lib/providers";
import { PaywallNote } from "@/components/billing/PaywallNote";

/* Transition briefs — the killer feature: turn a departing teammate's
   contributions into a queryable, cited brief. Data flow ported from
   archive/web-legacy-ui/screens/team.page.tsx's `run()` (POST /v1/queries,
   the same synthesis endpoint Ask uses) — restyled onto the mock's
   right-rail card. The mock's "N topics · N sources · N citations" line uses
   invented numbers; only citations are real on `AskResult`, so that's the
   only count shown. */
export function PeopleBrief({ members }: { members: Member[] }) {
  const { token, currentGraph } = useApp();
  const [departing, setDeparting] = useState("");
  const [askText, setAskText] = useState("");
  const [brief, setBrief] = useState<string | null>(null);
  const [citations, setCitations] = useState<number | null>(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  /** Set on a 402: the refusal reason, or null on an older backend. */
  const [paywall, setPaywall] = useState<{ reason: string | null } | null>(null);

  function pickDeparting(email: string) {
    setDeparting(email);
    setAskText(email ? `What was ${email}'s take on our key decisions?` : "");
  }

  async function run(text: string) {
    if (!token || !currentGraph || !text.trim()) return;
    setBusy(true);
    setErr(null);
    setPaywall(null);
    setBrief(null);
    setCitations(null);
    try {
      const r = await api.ask.run(token, currentGraph.id, text.trim());
      setBrief(r.answer);
      setCitations(r.citations.length);
    } catch (e) {
      if (e instanceof ApiError && e.status === 402) {
        setPaywall({ reason: e.refusalReason ?? null });
      } else {
        setErr(e instanceof ApiError ? e.message : "Query failed");
      }
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="flex flex-col gap-[16px]">
      <div>
        <Eyebrow className="mb-[8px]">offboarding</Eyebrow>
        <h2 className="mb-[6px] font-display text-[23px] font-normal leading-[1.25] text-ink">Transition briefs</h2>
        <p className="text-[13px] leading-[1.6] text-ink-2">
          Turn a departing teammate&rsquo;s topics into a queryable brief — synthesized from what they actually
          contributed to this workspace, with citations.
        </p>
      </div>

      <div>
        <Eyebrow className="mb-[7px]">departing teammate</Eyebrow>
        <Select value={departing} onChange={(e) => pickDeparting(e.target.value)}>
          <option value="">Select…</option>
          {members.map((m) => (
            <option key={m.user_id} value={m.email}>
              {m.email}
            </option>
          ))}
        </Select>
        <Button
          variant="brand"
          className="mt-[8px]"
          full
          disabled={!departing || busy}
          onClick={() => void run(`Summarize the key decisions, threads, and context contributed by ${departing}. Organize it as a transition brief.`)}
        >
          {busy ? "Synthesizing…" : "Generate transition brief"}
        </Button>
      </div>

      <div className="flex items-center gap-[8px]">
        <SearchInput
          icon={<IconSearch width={13} height={13} />}
          value={askText}
          onChange={(e) => setAskText(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && void run(askText)}
        />
        <Button size="sm" variant="primary" disabled={busy || !askText.trim()} onClick={() => void run(askText)}>
          Ask
        </Button>
      </div>

      {err && <ErrorNote>{err}</ErrorNote>}
      {paywall && <PaywallNote reason={paywall.reason} />}

      {brief && (
        <div className="rounded-control border border-line bg-bg p-[14px]">
          <div className="mb-[9px] flex items-center gap-[8px]">
            <Eyebrow>brief</Eyebrow>
            {citations !== null && (
              <Mono className="ml-auto text-ink-3">
                {citations} citation{citations === 1 ? "" : "s"}
              </Mono>
            )}
          </div>
          <p className="text-[13px] leading-[1.65] text-ink-2">{brief}</p>
        </div>
      )}
    </div>
  );
}
