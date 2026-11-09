"use client";

/* Whole-mailbox ingest ("everything mode") — Outlook only. The one permitted
 * exception to per-thread opt-in (docs/connectors/CONNECTOR_CONTRACT.md §7), so
 * turning it ON goes through a confirm card that says exactly what happens;
 * turning it OFF is immediate. Routes (routes/outlook_everything_mode.rs):
 *
 *   GET  /v1/connectors/microsoft/everything-mode            → {enabled}
 *   POST /v1/connectors/microsoft/everything-mode {enabled}  → caller's OWN mailbox only
 *
 * Copy is Settings.gs / Card.gs buildEverythingModeConfirmCard_, byte for byte. */

import { useCallback, useEffect, useRef, useState } from "react";
import { copy } from "@/lib/addin/copy";
import { FilledButton, Footer, OutlineButton, Row, Section, Switch, type Hue } from "./kit";
import type { EmailApi } from "@/lib/addin/emailTransport";

const PATH = "/v1/connectors/microsoft/everything-mode";

export function useEverythingMode({ api, toast, onUnauthorized }: { api: EmailApi; toast: (message: string) => void; onUnauthorized: () => void }) {
  /** null = cannot be determined (the switch is hidden rather than showing a state nobody can vouch for). */
  const [enabled, setEnabled] = useState<boolean | null>(null);
  const [busy, setBusy] = useState(false);
  const alive = useRef(true);
  useEffect(() => { alive.current = true; return () => { alive.current = false; }; }, []);

  const read = useCallback(async () => {
    try {
      const result = await api("GET", PATH);
      const value = result.ok && typeof result.data === "object" && result.data !== null ? (result.data as { enabled?: unknown }).enabled : undefined;
      if (alive.current) setEnabled(typeof value === "boolean" ? value : null);
    } catch { if (alive.current) setEnabled(null); }
  }, [api]);
  useEffect(() => { void read(); }, [read]);

  const write = useCallback(async (next: boolean) => {
    setBusy(true);
    try {
      const result = await api("POST", PATH, { enabled: next });
      if (!alive.current) return;
      if (result.status === 401 || result.unauthenticated) { onUnauthorized(); return; }
      toast(result.ok ? (next ? copy.mailbox.on : copy.mailbox.off) : result.status === 403 ? copy.mailbox.unavailable : copy.mailbox.failed(result.status));
    } catch { if (alive.current) toast(copy.email.unreachable); }
    finally { if (alive.current) setBusy(false); }
    await read(); // always show what the backend says, never what was asked for
  }, [api, onUnauthorized, read, toast]);

  return { enabled, busy, write };
}

export function MailboxRow({ enabled, busy, onToggle }: { enabled: boolean; busy: boolean; onToggle: (next: boolean) => void }) {
  return (
    <Section>
      <Row icon="inbox" hue="coral" text={copy.mailbox.row} trailing={<Switch checked={enabled} disabled={busy} onChange={onToggle} label={copy.mailbox.row} />} />
    </Section>
  );
}

/** Shown when the switch is turned ON. Nothing is written until "Turn on". */
export function MailboxConfirm({ onConfirm, onCancel }: { onConfirm: () => void; onCancel: () => void }) {
  return (
    <>
      <Section header={copy.mailbox.confirmHeader}>
        {copy.mailbox.rows.map((row) => <Row key={row.text} icon={row.icon} hue={row.hue as Hue} text={row.text} bottom={row.bottom} />)}
      </Section>
      <Footer>
        <OutlineButton onClick={onCancel}>{copy.mailbox.cancel}</OutlineButton>
        <FilledButton onClick={onConfirm}>{copy.mailbox.turnOn}</FilledButton>
      </Footer>
    </>
  );
}
