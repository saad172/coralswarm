import { parseToolkit } from "../components/connectors/composioModel";
import { pageRoutes } from "./pageRoutes";

/** An early bookmark redirect, before App Router can stream the app shell. */
export function pageRedirectDestination(pathname: string, search: string, context?: { audience?: "anonymous" | "authenticated" }): string | null {
  const entry = pageRoutes.find((route) => route.route === pathname) ?? pageRoutes.find((route) => {
    if (!route.route.includes("/[[...")) return false;
    const base = route.route.slice(0, route.route.indexOf("/[[..."));
    return pathname === base || pathname.startsWith(`${base}/`);
  });
  if (!entry || !("redirect" in entry)) return null;
  // An explicit measurement audience can include the destination's auth gate.
  // Middleware's two-argument bookmark redirect remains unchanged.
  if (context?.audience === "anonymous" && "anonymous" in entry.redirect) return entry.redirect.anonymous.to;
  const { to, query } = entry.redirect;
  const params = new URLSearchParams(search);
  if (query === "toolkit") {
    const toolkit = parseToolkit(params.get("toolkit"));
    return toolkit ? `${to}/${toolkit}` : null;
  }
  if (query === "missing-id") return params.get("id") ? null : to;
  if (query === "drop") return to;
  if (query === "installation_id") {
    const id = params.get("installation_id");
    return id ? `${to}?installation_id=${encodeURIComponent(id)}` : to;
  }
  return params.size ? `${to}?${params}` : to;
}
