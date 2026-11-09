import type { EntityKind, LibraryPage } from "@/lib/api";
import { api } from "@/lib/api";

export const PAGE_SIZE = 20;

export async function listLibraryPage(
  token: string,
  params: {
    graphId?: string;
    entityType?: EntityKind;
    q?: string;
    page?: number;
  },
  signal?: AbortSignal,
): Promise<LibraryPage> {
  return api.library.list(token, { ...params, pageSize: PAGE_SIZE }, signal);
}
