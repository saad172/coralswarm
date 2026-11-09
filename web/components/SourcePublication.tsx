"use client";

import { useEffect, useId, useRef, useState } from "react";
import { api, ApiError, type Graph, type SourceConnection } from "@/lib/api";
import { Button, ErrorNote, Modal } from "@/components/ui";
import type { PromoteModalProps } from "./PromoteModal";

import {
  CONNECTION_POLL_MS,
  connectionState,
  onTabHidden,
  onTabVisible,
  pollActivity,
  pollModeAfter,
  type PollActivity,
  type PollMode,
} from "@/lib/sourcePublication";

export function SourceConnections({
  token,
  graphId,
  graphs,
  refresh = 0,
}: {
  token: string | null;
  graphId: string;
  graphs: Graph[];
  refresh?: number;
}) {
  const [pages, setPages] = useState<(string | undefined)[]>([undefined]);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const before = pages[pages.length - 1];
  const [rows, setRows] = useState<SourceConnection[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [revision, setRevision] = useState(0);
  // True once the poll has given up after repeated failures — surfaced with a
  // manual "Check again" so a person is never left watching frozen numbers
  // with no way back.
  const [pollStopped, setPollStopped] = useState(false);
  // The poll is self-scheduling (not a fixed interval) because its cadence has
  // to react to what it just read. Every decision — the cadence, the failure
  // backoff, the two terminal states, and what a tab switch does to each of
  // them — lives in `lib/sourcePublication.ts`; this effect is the timer and
  // fetch wiring around it, per the same split `useJobPoll` uses.
  // The loop reads the CURRENT bearer each poll and restarts only when auth
  // appears or goes, not on every rotation (which reset its backoff).
  const tokenRef = useRef(token);
  tokenRef.current = token;
  const hasToken = !!token;
  useEffect(() => {
    if (!hasToken) return;
    let active = true;
    let timer: ReturnType<typeof setTimeout> | null = null;
    let failures = 0;
    // Assume work is in flight until a reading says otherwise, so the first
    // scheduling decision after a mount is never "settled, stop".
    let activity: PollActivity = "working";
    let mode: PollMode = "running";
    let inFlight = false;
    let lastPollAt = 0;
    const hidden = () => typeof document !== "undefined" && document.hidden;
    const clearPending = () => {
      if (timer) clearTimeout(timer);
      timer = null;
    };

    const schedule = () => {
      if (!active) return;
      const next = pollModeAfter(activity, failures, hidden());
      mode = next.mode;
      // `gaveUp` is the only state with a control attached; everything else
      // recovers on its own or through an action that re-runs this effect.
      setPollStopped(next.mode === "gaveUp");
      if (next.delayMs !== null) timer = setTimeout(() => void poll(), next.delayMs);
    };

    const poll = async () => {
      timer = null;
      if (!active) return;
      // A timer can fire in the same tick the tab is hidden, before the
      // listener below runs. Suspend rather than spend the request.
      if (hidden()) {
        mode = "hidden";
        return;
      }
      inFlight = true;
      lastPollAt = Date.now();
      try {
        const bearer = tokenRef.current;
        if (!bearer) throw new Error("Not authenticated");
        // eslint-disable-next-line no-restricted-syntax -- an adaptive poll loop, not a one-shot read: cadence, failure backoff, terminal states and tab-visibility suspension live in lib/sourcePublication.ts, which a cached query cannot express
        const result = await api.sourceConnections.list(bearer, graphId, before);
        failures = 0;
        activity = pollActivity(result.connections);
        if (active) {
          setRows(result.connections);
          setNextCursor(result.next_cursor);
          setError(null);
        }
      } catch (e) {
        failures += 1;
        if (active)
          setError(
            e instanceof ApiError
              ? e.message
              : "Connection status is unavailable. Last known results are shown.",
          );
      } finally {
        inFlight = false;
        if (active) schedule();
      }
    };

    // Leaving the tab suspends a RUNNING loop and drops its pending timer —
    // leaving one armed is pointless (browsers throttle background timers to
    // roughly once a minute) and it hid the staleness on the way back. A
    // terminal loop is untouched: `settled` and `gaveUp` do not come back
    // from a tab switch.
    const onVisibility = () => {
      if (!active) return;
      if (hidden()) {
        const next = onTabHidden(mode);
        if (next.clearTimer) clearPending();
        mode = next.mode;
        return;
      }
      const next = onTabVisible(mode);
      mode = next.mode;
      if (!next.pollNow) return;
      // A request that outlived the hide will schedule from its own `finally`
      // now that we are visible again; starting a second one here would just
      // duplicate it.
      if (inFlight) return;
      // What is on screen is as old as the tab was hidden, so take a fresh
      // reading — but never more than one per base interval, so hide/show
      // flapping cannot turn into a request storm. (This can pull a failing
      // poll's next attempt in ahead of its backoff; `failures` is NOT reset,
      // so the ladder still runs out at `CONNECTION_POLL_MAX_FAILURES`.)
      const since = Date.now() - lastPollAt;
      if (since >= CONNECTION_POLL_MS) void poll();
      else timer = setTimeout(() => void poll(), CONNECTION_POLL_MS - since);
    };
    if (typeof document !== "undefined")
      document.addEventListener("visibilitychange", onVisibility);

    void poll();
    return () => {
      active = false;
      clearPending();
      if (typeof document !== "undefined")
        document.removeEventListener("visibilitychange", onVisibility);
    };
  }, [hasToken, graphId, refresh, revision, before]);
  async function update(c: SourceConnection, action: "pause" | "resume" | "retry") {
    if (!token) return;
    setBusy(c.id);
    try {
      await api.sourceConnections.update(token, graphId, c.id, action);
      setRevision((v) => v + 1);
    } catch (e) {
      setError(e instanceof ApiError ? e.message : "Could not change this connection.");
    } finally {
      setBusy(null);
    }
  }
  if (!rows.length && !error && pages.length === 1) return null;
  return (
    <section className="my-[20px] rounded-card border border-line bg-bg p-[16px]">
      <h2 className="text-[14px] font-medium">Source publications</h2>
      <p className="mt-1 text-[12px] text-ink-3">
        Connected sources publish new evidence and revisions automatically. Pausing stops future
        copies; published evidence stays in the workspace.
      </p>
      {error && <ErrorNote>{error}</ErrorNote>}
      {pollStopped && (
        <div className="mt-2 flex flex-wrap items-center gap-2 text-[12px] text-ink-3">
          <span>Live status updates stopped after repeated errors.</span>
          <Button
            size="sm"
            variant="secondary"
            onClick={() => {
              setPollStopped(false);
              setRevision((v) => v + 1);
            }}
          >
            Check again
          </Button>
        </div>
      )}
      {rows.map((c) => (
        <div key={c.id} className="mt-3 border-t border-line-2 pt-3">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div className="min-w-0">
              <div className="break-words text-[13px] font-medium">
                {c.label} →{" "}
                {graphs.find((g) => g.id === c.target_graph_id)?.name ?? "Shared workspace"}
              </div>
              <div className="mt-1 text-[12px] text-ink-2">
                {error ? "Status unavailable" : connectionState(c)} ·{" "}
                {c.mode === "live" ? "Connected" : "One-time publication"}
              </div>
            </div>
            <div className="flex gap-2">
              {c.processing?.state === "attention" && (
                <Button
                  size="sm"
                  variant="secondary"
                  disabled={busy === c.id}
                  onClick={() => update(c, "retry")}
                >
                  Retry processing
                </Button>
              )}
              {(c.mode === "live" ||
                c.has_pending_evidence ||
                c.processing?.state === "processing" ||
                c.paused) && (
                <Button
                  size="sm"
                  variant="secondary"
                  disabled={busy === c.id}
                  onClick={() => update(c, c.paused ? "resume" : "pause")}
                >
                  {c.paused ? "Resume" : "Pause"}
                </Button>
              )}
            </div>
          </div>
          {c.error_message && (
            <p className="mt-2 text-[12px] text-ink-2">
              Publication cannot continue. Check your source and workspace access, then resume.
            </p>
          )}
          <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-[11px] text-ink-3">
            {["copying", "building", "checking"].map((name) => {
              const stage = c.processing?.stages.find((s) => s.stage === name);
              return stage ? (
                <span key={name}>
                  {name === "copying"
                    ? "Copying evidence"
                    : name === "building"
                      ? "Topics and relationships"
                      : "Follow-up checks"}
                  : {stage.completed}/{stage.total}
                  {stage.failed ? ` · ${stage.failed} failed` : ""}
                  {stage.skipped ? ` · ${stage.skipped} needed no changes` : ""}
                </span>
              ) : null;
            })}
          </div>
          {c.last_checked_at && (
            <p className="mt-1 text-[10px] text-ink-3">
              Last checked {new Date(c.last_checked_at).toLocaleString()}
            </p>
          )}
        </div>
      ))}
      {(pages.length > 1 || nextCursor) && (
        <div className="mt-4 flex items-center justify-between">
          <Button
            size="sm"
            variant="secondary"
            disabled={pages.length === 1}
            onClick={() => setPages((p) => p.slice(0, -1))}
          >
            Previous
          </Button>
          <span className="text-[11px] text-ink-3">Page {pages.length}</span>
          <Button
            size="sm"
            variant="secondary"
            disabled={!nextCursor}
            onClick={() => {
              if (nextCursor) setPages((p) => [...p, nextCursor]);
            }}
          >
            Next
          </Button>
        </div>
      )}
    </section>
  );
}

export function SourcePromotionModal({ target, oceans, token, onClose }: PromoteModalProps) {
  const [destination, setDestination] = useState("");
  const [live, setLive] = useState(false);
  const [channel, setChannel] = useState(false);
  const [history, setHistory] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [submitted, setSubmitted] = useState(false);
  const [created, setCreated] = useState<Set<string>>(new Set());
  const [revision, setRevision] = useState(0);
  const modeGroup = `publication-mode-${useId()}`;
  if (target.kind !== "sources") return null;
  const t = target;
  const destinations = oceans.filter(
    (g) => (g.kind === "team" || g.kind === "org") && g.id !== t.graphId,
  );
  const channelAvailable = t.sources.every((s) => /^[CG][A-Z0-9]+\//.test(s.id));
  async function submit() {
    if (!token || !destination || busy) return;
    setBusy(true);
    setError(null);
    const successes = new Set(created);
    const failures: string[] = [];
    for (const source of t.sources) {
      const key = `${destination}:${source.id}`;
      if (successes.has(key)) continue;
      try {
        await api.sourceConnections.create(token, t.graphId, {
          source_id: source.id,
          target_graph_id: destination,
          keep_connected: live,
          channel,
          include_history: live ? history : true,
        });
        successes.add(key);
      } catch (e) {
        failures.push(
          `${source.label}: ${e instanceof ApiError ? e.message : "Could not start publication"}`,
        );
      }
    }
    setCreated(successes);
    setRevision((v) => v + 1);
    setBusy(false);
    if (failures.length) setError(failures.join(" · "));
    else setSubmitted(true);
  }
  return (
    <Modal
      open
      title={submitted ? "Source publication started" : "Publish sources to a workspace"}
      onClose={() => onClose()}
      width={680}
      footer={
        <>
          <Button variant="secondary" onClick={() => onClose()}>
            Close
          </Button>
          {!submitted && (
            <Button variant="brand" disabled={!destination || busy} onClick={submit}>
              {busy ? "Starting…" : live ? "Connect source" : "Publish once"}
            </Button>
          )}
        </>
      }
    >
      {submitted ? (
        <p className="text-[13px] text-ink-2">
          You can close this dialog. Follow processing and manage the connection in Library →
          Sources.
        </p>
      ) : (
        <div className="space-y-4 text-[13px]">
          <p>
            Publish the selected evidence. The workspace builds its own topics and relationships
            from that evidence.
          </p>
          <div className="max-h-40 overflow-auto rounded-card border border-line p-3">
            {t.sources.map((s) => (
              <div key={s.id}>{s.label}</div>
            ))}
          </div>
          <label className="block">
            Destination workspace
            <select
              className="mt-1 block w-full rounded-control border border-line bg-bg p-2"
              value={destination}
              disabled={busy || created.size > 0}
              onChange={(e) => setDestination(e.target.value)}
            >
              <option value="">Choose a workspace</option>
              {destinations.map((g) => (
                <option key={g.id} value={g.id}>
                  {g.name}
                </option>
              ))}
            </select>
          </label>
          <fieldset disabled={busy || created.size > 0} className="space-y-3">
            {/* One `name` across both inputs is what makes these a single radio
                group: without it each is its own one-option group, arrow keys
                don't move between them, and assistive tech announces two
                unrelated controls instead of "1 of 2". `useId` keeps the name
                unique if a second copy of this modal is ever mounted. The
                nested fieldset + screen-reader-only legend gives the group the
                label the visible layout never had. */}
            <fieldset className="space-y-3">
              <legend className="sr-only">How should this source publish?</legend>
              <label className="flex gap-2">
                <input
                  type="radio"
                  name={modeGroup}
                  checked={!live}
                  onChange={() => setLive(false)}
                />
                Publish once
              </label>
              <label className="flex gap-2">
                <input type="radio" name={modeGroup} checked={live} onChange={() => setLive(true)} />
                Keep this source connected and publish future updates automatically
              </label>
            </fieldset>
            {live && (
              <label className="flex gap-2">
                <input
                  type="checkbox"
                  checked={history}
                  onChange={(e) => setHistory(e.target.checked)}
                />
                Also publish existing evidence
              </label>
            )}
            {channelAvailable && (
              <label className="flex gap-2">
                <input
                  type="checkbox"
                  checked={channel}
                  onChange={(e) => setChannel(e.target.checked)}
                />
                Include the entire Slack channel, including future messages when connected
              </label>
            )}
          </fieldset>
          <p className="text-[12px] text-ink-3">
            {channel ? "Channel" : "Selected source item"} evidence becomes visible to members of
            the destination workspace. Evidence from other sources stays outside this publication.
          </p>
        </div>
      )}
      {error && <ErrorNote>{error}</ErrorNote>}
      <SourceConnections token={token} graphId={t.graphId} graphs={oceans} refresh={revision} />
    </Modal>
  );
}
