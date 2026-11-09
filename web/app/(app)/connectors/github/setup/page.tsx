import { redirect } from "next/navigation";
import type { PageSearchParams } from "@/lib/pageRedirect";

// GitHub's setup URL preserves only its existing installation selector.
export default async function GithubSetupPage({ searchParams }: { searchParams: PageSearchParams }) {
  const value = (await searchParams).installation_id;
  const id = Array.isArray(value) ? value[0] : value;
  redirect(id ? `/connectors/github?installation_id=${encodeURIComponent(id)}` : "/connectors/github");
}
