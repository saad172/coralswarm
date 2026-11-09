import { NextResponse, type NextRequest } from "next/server";
import { pageRedirectDestination } from "./lib/pageRedirectDestination";

// Resolve retired bookmarks before streaming can mount the current-account
// shell. Authorization remains at each destination's existing app gate.
export function middleware(request: NextRequest) {
  const destination = pageRedirectDestination(request.nextUrl.pathname, request.nextUrl.search);
  return destination ? NextResponse.redirect(new URL(destination, request.url), 307) : NextResponse.next();
}
export const config = { matcher: ["/((?!_next|api|favicon.ico|robots.txt|sitemap.xml).*)"] };
