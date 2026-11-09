import { redirect } from "next/navigation";

export type PageSearchParams = Promise<Record<string, string | string[] | undefined>>;

/** Resolve retired bookmarks before mounting an authenticated shell. The
 * destination receives the same query values, including repeated selectors. */
export async function redirectPage(to: string, searchParams: PageSearchParams, preserveQuery = true): Promise<never> {
  const query = new URLSearchParams();
  if (preserveQuery) for (const [key, value] of Object.entries(await searchParams)) {
    if (Array.isArray(value)) value.forEach((item) => query.append(key, item));
    else if (value !== undefined) query.append(key, value);
  }
  redirect(query.size ? `${to}?${query}` : to);
}
