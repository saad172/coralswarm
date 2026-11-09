"use client";

import { useCallback, useRef, useState } from "react";
import { ApiError, type ConversationSummary } from "@/lib/api";
import { useEvidenceAdmission } from "@/lib/useEvidenceAdmission";
import { deleteScopedConversation } from "@/lib/scopedChatApi";
import { emitConversationsChanged } from "@/lib/conversationsChanged";
import { Button, ErrorNote, IconButton, Modal } from "@/components/ui";
import { IconTrash } from "@/components/icons";

/* Delete a chat. One flow, used by every surface that lists one: the sidebar
   history rows and the Chats landing list (this icon), and the open
   conversation's header (its "…" menu, via `useDeleteChat`).

   Two things are deliberately owned HERE rather than by the three call sites:

   - Whether the control exists at all. `DELETE /v1/conversations/{id}` is
     owner-only, so a conversation shared INTO your account (`mine: false`) has
     no delete — a rule that would eventually be forgotten by one of the
     surfaces if each decided for itself.
   - The confirmation. The backend hard-deletes, messages cascade, and there is
     no undo anywhere in the product, so this asks first — in a real Modal,
     which the kit reserves for exactly this kind of deliberate, irreversible
     act (see ui/Modal.tsx).

   Nothing here refreshes any list: it announces the delete
   (lib/conversationsChanged.ts) and each surface reacts, including the Chats
   screen abandoning the thread if the deleted chat is the one on screen. */

/** The delete flow without its trigger: whether this viewer may delete `conv`,
 *  a `request` that opens the confirmation, and the dialog to render. The
 *  row icon below and the chat header's "…" menu both open the same dialog. */
export function useDeleteChat(conv: ConversationSummary | null) {
  const { key: admissionKey, ready: admissionReady, getToken } = useEvidenceAdmission();
  const key = JSON.stringify([admissionKey, conv?.id ?? null]);
  const activeKey = useRef(key); activeKey.current = key;
  const [dialogKey, setDialogKey] = useState<string | null>(null);
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const remove = useCallback(async () => {
    if (!conv || !admissionReady || activeKey.current !== key || busy) return;
    setBusy(true);
    setErr(null);
    try {
      await deleteScopedConversation(getToken(), conv.id, new AbortController().signal);
      if (activeKey.current !== key) return;
      getToken();
      emitConversationsChanged(conv.id);
      setOpen(false);
    } catch (e) {
      // Kept in the dialog rather than closing on failure: closing would read
      // as success, and the row is still there to try again.
      if (activeKey.current !== key) return;
      setErr(e instanceof ApiError ? e.message : "Couldn't delete that chat.");
    } finally {
      if (activeKey.current === key) setBusy(false);
    }
  }, [admissionReady, getToken, key, busy, conv]);

  const available = !!conv?.mine && admissionReady;
  const title = conv?.title?.trim() || "Untitled";
  const request = useCallback(() => {
    try { getToken(); } catch { return; }
    setDialogKey(key);
    setErr(null);
    setOpen(true);
  }, [getToken, key]);

  const dialog = available ? (
    <Modal
      open={open && dialogKey === key}
      onClose={() => (busy ? undefined : setOpen(false))}
      title="Delete this chat?"
      description={`“${title}” and its answers are deleted for good. Anything you promoted into a workspace stays.`}
      width={440}
      footer={
        <>
          <Button size="sm" onClick={() => setOpen(false)} disabled={busy}>
            Cancel
          </Button>
          <Button size="sm" variant="danger" onClick={remove} disabled={busy}>
            {busy ? "Deleting…" : "Delete"}
          </Button>
        </>
      }
    >
      {err && <ErrorNote>{err}</ErrorNote>}
    </Modal>
  ) : null;

  return { available, title, request, dialog };
}

export function DeleteChatButton({
  conv,
  size = 22,
  className,
}: {
  conv: ConversationSummary;
  /** Square size of the icon control, to sit in rows of different heights. */
  size?: number;
  className?: string;
}) {
  const { available, title, request, dialog } = useDeleteChat(conv);
  if (!available) return null;

  return (
    <>
      <IconButton
        label={`Delete chat “${title}”`}
        size={size}
        className={className}
        onClick={(e) => {
          // The control sits alongside a row-wide <Link>; without this a click
          // both opens the dialog and navigates into the chat behind it.
          e.preventDefault();
          e.stopPropagation();
          request();
        }}
      >
        <IconTrash width={Math.round(size * 0.55)} height={Math.round(size * 0.55)} />
      </IconButton>
      {dialog}
    </>
  );
}
