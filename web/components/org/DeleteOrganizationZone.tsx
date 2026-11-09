import Link from "next/link";
import { ORG_DELETION_PATH } from "./orgDeletionModel";

/* Organization -> Billing danger zone: the way in to in-app org deletion
   (backend control 0387). It only links: `/org/deletion` checks who the
   caller is (owner only), whether the feature is on, and explains what
   happens before anything is confirmed. */
export function DeleteOrganizationZone() {
  return (
    <section aria-label="Delete organization" className="mt-[18px] rounded-card border border-bad/35 px-[15px] py-[13px]">
      <div className="text-[13.5px] font-medium text-ink">Delete organization</div>
      <div className="mt-[3px] flex items-center gap-[14px]">
        <p className="min-w-0 flex-1 text-[12.5px] leading-[1.5] text-ink-3">
          Owner only. Access ends at once for everyone and the subscription is cancelled with no refund. You
          then have 7 days to download the data or undo.
        </p>
        <Link
          href={ORG_DELETION_PATH}
          className="flex-none text-[12.5px] font-medium text-bad underline underline-offset-2"
        >
          Delete organization…
        </Link>
      </div>
    </section>
  );
}
