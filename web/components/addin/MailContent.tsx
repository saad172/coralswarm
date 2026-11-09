"use client";

/* What the Outlook email pane shows once there is a way to call the API, shared
 * by the SSO path and the Clerk fallback: the open message's save card, the home
 * state when no message is open, and the whole-mailbox switch + confirm card. */

import { useCallback, useMemo, useState } from "react";
import type { EmailApi } from "@/lib/addin/emailTransport";
import type { OfficeApi } from "@/lib/addin/office";
import { EmailPane } from "./EmailPane";
import { MailboxConfirm, MailboxRow, useEverythingMode } from "./MailboxSetting";
import { HomeBody } from "./screens";
import type { ItemSnapshot } from "./useOfficeItem";

export function MailContent({ api, item, office, workspaceName, toast, onUnauthorized, openToday }: {
  api: EmailApi;
  item: ItemSnapshot;
  office: OfficeApi | null;
  workspaceName: string;
  toast: (message: string) => void;
  onUnauthorized: () => void;
  openToday: () => void;
}) {
  const [confirming, setConfirming] = useState(false);
  const mailbox = useEverythingMode({ api, toast, onUnauthorized });
  const toRestId = useCallback((ewsId: string) => {
    if (!office) throw new Error("Office is not ready");
    return office.context.mailbox.convertToRestId(ewsId, office.MailboxEnums.RestVersion.v2_0);
  }, [office]);
  const emailItem = useMemo(() => (item.kind === "message" ? { itemId: item.itemId, conversationId: item.conversationId, subject: item.subject, from: item.from } : null), [item]);

  if (confirming) {
    return <MailboxConfirm onCancel={() => setConfirming(false)} onConfirm={() => { setConfirming(false); void mailbox.write(true); }} />;
  }
  return (
    <>
      {emailItem && item.kind === "message" ? (
        <EmailPane key={`${item.key}`} item={emailItem} api={api} workspaceName={workspaceName} toast={toast} onUnauthorized={onUnauthorized} toRestId={toRestId} openToday={openToday} />
      ) : <HomeBody surface="email" />}
      {mailbox.enabled !== null ? (
        <MailboxRow enabled={mailbox.enabled} busy={mailbox.busy} onToggle={(next) => { if (next) setConfirming(true); else void mailbox.write(false); }} />
      ) : null}
    </>
  );
}
