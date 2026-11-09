"use client";

import type { ReactNode } from "react";
import { Button, ErrorNote, Modal } from "@/components/ui";

/* The confirm step for a destructive action: the kit's Modal with a Cancel
   and a danger button, plus a place for the failure.

   The dialog stays open when the action fails and shows the error inside it:
   closing would read as success. The caller decides when to close (on
   success), so nothing here fires the action itself except the confirm
   button, and Cancel / Escape / backdrop never do. */
export function ConfirmDialog({
  open,
  title,
  description,
  confirmLabel,
  busyLabel,
  busy = false,
  confirmDisabled = false,
  error = null,
  onConfirm,
  onCancel,
  children,
}: {
  open: boolean;
  title: ReactNode;
  description?: ReactNode;
  confirmLabel: string;
  busyLabel?: string;
  busy?: boolean;
  /** Extra reason to keep the confirm button off (e.g. a typed-confirmation not yet matching). */
  confirmDisabled?: boolean;
  error?: string | null;
  onConfirm: () => void;
  onCancel: () => void;
  children?: ReactNode;
}) {
  return (
    <Modal
      open={open}
      onClose={() => (busy ? undefined : onCancel())}
      title={title}
      description={description}
      width={460}
      footer={
        <>
          <Button size="sm" onClick={onCancel} disabled={busy}>
            Cancel
          </Button>
          <Button size="sm" variant="danger" onClick={onConfirm} disabled={busy || confirmDisabled}>
            {busy ? (busyLabel ?? "Working…") : confirmLabel}
          </Button>
        </>
      }
    >
      {children}
      {error && <ErrorNote>{error}</ErrorNote>}
    </Modal>
  );
}
