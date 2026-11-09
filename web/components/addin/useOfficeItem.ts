"use client";

/* The Outlook item the pane is looking at, kept current through ItemChanged (a
 * pinned task pane stays open while the user moves between messages). Only
 * minimal fields are read: ids, subject, sender name. Never the body. */

import { useEffect, useState } from "react";
import { mark } from "@/lib/addin/perf";
import type { OfficeApi, OfficeMailItem } from "@/lib/addin/office";

export type ItemSnapshot =
  | { kind: "loading" }
  | { kind: "none" }
  | { kind: "message"; key: string; itemId: string; conversationId: string | null; subject: string | null; from: string | null }
  | { kind: "appointment"; key: string; compose: boolean };

/** Compose (organizer) mode exposes the subject as an async object instead of a string. */
export function isComposeItem(item: OfficeMailItem): boolean {
  return typeof item.subject === "object" && item.subject !== null && typeof item.subject.getAsync === "function";
}

export function snapshotOf(office: OfficeApi, item: OfficeMailItem | null): ItemSnapshot {
  if (!item) return { kind: "none" };
  const types = office.MailboxEnums.ItemType;
  if (item.itemType === types.Appointment) {
    // An unsaved draft has no itemId; the caller disambiguates drafts with a sequence number (see useOfficeItem).
    return { kind: "appointment", key: `appt:${item.itemId ?? ""}`, compose: isComposeItem(item) };
  }
  if (item.itemType === types.Message && item.itemId) {
    return {
      kind: "message", key: `msg:${item.itemId}`, itemId: item.itemId, conversationId: item.conversationId ?? null,
      subject: typeof item.subject === "string" ? item.subject : null, from: item.from?.displayName || item.from?.emailAddress || null,
    };
  }
  return { kind: "none" };
}

export function useOfficeItem(getOffice: () => Promise<OfficeApi>): { office: OfficeApi | null; item: ItemSnapshot } {
  const [office, setOffice] = useState<OfficeApi | null>(null);
  const [item, setItem] = useState<ItemSnapshot>({ kind: "loading" });
  useEffect(() => {
    let live = true;
    let handler: (() => void) | null = null;
    let current: OfficeApi | null = null;
    getOffice().then((loaded) => {
      if (!live) return;
      current = loaded;
      setOffice(loaded);
      let drafts = 0;
      const read = () => {
        if (!live) return;
        const next = snapshotOf(loaded, loaded.context.mailbox.item);
        // Unsaved drafts share an empty itemId: give each ItemChanged its own key so state never leaks between drafts.
        if (next.kind === "appointment" && next.compose) next.key = `compose:${++drafts}`;
        setItem(next);
      };
      read();
      mark("item-ready");
      handler = read;
      loaded.context.mailbox.addHandlerAsync?.(loaded.EventType.ItemChanged, read);
    }).catch(() => { if (live) setItem({ kind: "none" }); });
    return () => {
      live = false;
      if (current && handler) current.context.mailbox.removeHandlerAsync?.(current.EventType.ItemChanged, handler);
    };
    // The loader is a stable per-page function.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
  return { office, item };
}
