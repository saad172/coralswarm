import { CancelledError, type QueryClient } from "@tanstack/react-query";
import { connectorsQueryOptions, conversationsQueryOptions, type QueryCtx } from "./appQueries";
import { homeAttentionQueryOptions, homeContinueQueryOptions, homeWhatsNewQueryOptions } from "./homeLaunchpadApi";
import { oceansIndexQueryOptions } from "./oceansIndex";
import { pageReadToken, verifyPageReadToken } from "./pageReadAuthority";

/** Only parameter-free landing reads can precede shell metadata. In
 * particular, Today needs bootstrap's feature flags and selected ocean.
 * Keep the canonical factories: authority/refusal fences, fan-in certificates,
 * refresh budgets and cache keys must match the eventual page observers. */
export function beginInitialPageReads(client: QueryClient, pathname: string, ctx: QueryCtx, search = "") {
  const pageOptions = pathname === "/home"
    ? [homeContinueQueryOptions(ctx), homeWhatsNewQueryOptions(ctx), homeAttentionQueryOptions(ctx)]
    : pathname === "/connectors" ? [connectorsQueryOptions(ctx)]
    : pathname === "/workspaces" && !new URLSearchParams(search).get("id") ? [oceansIndexQueryOptions(ctx)] : [];
  const options = [...pageOptions, ...(["/home", "/connectors", "/workspaces", "/today"].includes(pathname) ? [conversationsQueryOptions(ctx)] : [])];
  let active = true;
  let release!: () => void;
  const admitted = new Promise<void>((resolve) => { release = resolve; });
  const started = options.filter((option) => !client.getQueryCache().find({ queryKey: option.queryKey, exact: true }));
  for (const option of started) {
    const queryFn = option.queryFn;
    void client.prefetchQuery({ ...option, queryFn: async (context) => {
      // Enter the canonical factory synchronously: it registers the query's
      // authority/publication ticket before TanStack finishes starting it.
      const token = ctx.getAuthToken();
      const answer = await queryFn(context);
      // Leave the retryer in flight until the admitted page mounts. Home's
      // refetch-on-mount then joins this read instead of issuing a duplicate.
      await admitted;
      if (!active || context.signal.aborted) throw new CancelledError();
      // Also cover identity changes while waiting for shell admission.
      verifyPageReadToken(token, await pageReadToken(ctx.getAuthToken));
      return answer;
    } });
  }
  return {
    admit: release,
    cancel() {
      active = false;
      release();
      for (const option of started) {
        void client.cancelQueries({ queryKey: option.queryKey, exact: true }, { revert: false, silent: true });
        client.removeQueries({ queryKey: option.queryKey, exact: true });
      }
    },
  };
}
