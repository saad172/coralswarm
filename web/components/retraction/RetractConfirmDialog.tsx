import { Button, ErrorNote, Modal } from "@/components/ui";
import { RETRACT_DIALOG_BODY, RETRACT_DIALOG_TITLE, retractDialogDescription } from "@/lib/retraction";

/* D6's promise text (docs/plans/capture/02-b2-retract-fixes.md): "Taking it
   back: delete the file → retract" — the words "any time" and any
   implication of a full cascade are struck, because both are false under
   D5 (FM-3's 14-day rewrite window, FM-4's ex-member case). This is the
   "moment of decision" dialog for the retract act itself: it must never
   promise more reach than the product actually has, and every claim it
   makes has to be one the D6 UI can name a reason for it not meeting.
   Copy itself lives in lib/retraction.ts (retraction.test.ts asserts on it
   directly — see that module's own comment for why). */

export function RetractConfirmDialog({
  open,
  onClose,
  onConfirm,
  label,
  busy,
  error,
}: {
  open: boolean;
  onClose: () => void;
  onConfirm: () => void;
  /** The thing being retracted, named plainly — a checkpoint's own label. */
  label: string;
  busy?: boolean;
  /** A failed retract's error message (independent review of PR #729,
   *  finding 9, MEDIUM). Rendered INSIDE the dialog body — the caller used
   *  to render this on the page BEHIND the modal's scrim (`fixed inset-0
   *  z-50`) while the dialog itself stayed open, so a failed retract was
   *  invisible: the user saw the button flicker back to "Retract" and
   *  nothing else, with no signal to stop them retrying an operation that
   *  may have partially committed. */
  error?: string | null;
}) {
  return (
    <Modal
      open={open}
      onClose={onClose}
      title={RETRACT_DIALOG_TITLE}
      description={retractDialogDescription(label)}
      footer={
        <>
          <Button variant="ghost" onClick={onClose} disabled={busy}>
            Cancel
          </Button>
          <Button variant="danger" onClick={onConfirm} disabled={busy}>
            {busy ? "Retracting…" : "Retract"}
          </Button>
        </>
      }
    >
      <div className="flex flex-col gap-[10px]">
        <p className="text-[12.5px] leading-[1.6] text-ink-2">{RETRACT_DIALOG_BODY}</p>
        {error && <ErrorNote>{error}</ErrorNote>}
      </div>
    </Modal>
  );
}
