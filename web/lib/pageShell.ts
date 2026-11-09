// Canonical plain page reads. Stored model paths come only from the Rust
// registry. Interactive reads/mutations retain their action clients.
import type { RequestPriority } from "./transport";
import { pageReadPriority } from "./pageReadPriority";
import { apiRequest } from "./api";
import { authorizedPageRead, pageReadToken, verifyPageReadToken, pageReadPublicationBoundary, type PageReadContext } from "./pageReadAuthority";
import { STALE } from "./queryClient";
import type { QueryKey } from "@tanstack/react-query";

export { PLAIN_LOAD_PATHS, FAN_IN_LOAD_PATHS, SHELL_LOAD_PATHS, plainLoadPath } from "./pageShellPaths";
export type { PlainLoadPath, PlainParams } from "./pageShellPaths";
import { plainLoadPath, type PlainLoadPath, type PlainParams } from "./pageShellPaths";

export function plainPageSectionQuery<P>(
  path: PlainLoadPath, scope: string, params: PlainParams,
  getToken: () => string | null | Promise<string | null>,
  options: {
    queryKey?: QueryKey; public?: boolean; priority?: RequestPriority;
    /** Alternate representations can share permission refusal, not payloads. */
    authorityPath?: PlainLoadPath;
    /** Validate/transform before the guarded read can admit a result. */
    parse?: (body: unknown) => P;
  } = {},
) {
  const readIdentity = JSON.stringify([scope, plainLoadPath(options.authorityPath ?? path, params)]);
  const publication = pageReadPublicationBoundary();
  return {
    queryKey: options.queryKey ?? [path, scope, params],
    structuralSharing: publication.structuralSharing,
    queryFn: async (context: PageReadContext): Promise<P> => {
      publication.observe(context);
      const { signal } = context;
      const priority = pageReadPriority(context, options.priority ?? "critical");
      try {
        return await authorizedPageRead(context, readIdentity, async () => {
        const token = await pageReadToken(getToken, options.public);
        const body = await apiRequest<unknown>(plainLoadPath(path, params), token, undefined, false, { signal, priority: priority.source.current(), prioritySource: priority.source });
        verifyPageReadToken(token, await pageReadToken(getToken, options.public));
        return options.parse ? options.parse(body) : body as P;
        });
      } finally { priority.release(); }
    },
    meta: { pageRead: true, scope, plain: path, readIdentity, priority: options.priority ?? "critical" },
    staleTime: STALE.READ,
    retry: false as const,
  };
}
