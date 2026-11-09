import { redirectPage, type PageSearchParams } from "@/lib/pageRedirect";

// Preserve the retired bookmark's destination before shell reads mount.
export default function RetiredPage({ searchParams }: { searchParams: PageSearchParams }) {
  return redirectPage("/onboarding/connectors", searchParams, true);
}
