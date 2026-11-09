import { redirectPage, type PageSearchParams } from "@/lib/pageRedirect";

export default function RetiredSettingsPage({ searchParams }: { searchParams: PageSearchParams }) {
  return redirectPage("/settings/connections", searchParams);
}
