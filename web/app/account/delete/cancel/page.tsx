import type { Metadata } from "next";
import { CancelDeletionByLink } from "@/components/settings/CancelDeletionByLink";

// The emailed link carries a single-use token in the URL fragment. Never leak
// this page's URL to anything it links to or loads.
export const metadata: Metadata = {
  title: "Cancel account deletion",
  referrer: "no-referrer",
};

export default function AccountDeletionCancelPage() {
  return <CancelDeletionByLink />;
}
