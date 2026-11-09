import type { Metadata } from "next";
import { Instrument_Serif, Geist, JetBrains_Mono } from "next/font/google";
import { ClerkProvider } from "@clerk/nextjs";
import { ui } from "@clerk/ui";
import "./globals.css";
import { AppProvider } from "@/lib/providers";
import { clerkAppearance } from "@/lib/clerkAppearance";
import { HISTORY_GUARD_SCRIPT } from "@/lib/addin/history";

// The tree is wrapped in <ClerkProvider> (ADR-0007 D4).

// Design-system fonts (docs/design/DESIGN_SYSTEM.md):
// display = Instrument Serif (400, normal + italic), sans = Geist, mono = JetBrains Mono.
const displayFont = Instrument_Serif({
  subsets: ["latin"],
  weight: "400",
  style: ["normal", "italic"],
  variable: "--font-display",
  display: "swap",
});
const sans = Geist({ subsets: ["latin"], variable: "--font-sans", display: "swap" });
const mono = JetBrains_Mono({ subsets: ["latin"], variable: "--font-mono", display: "swap" });

export const metadata: Metadata = {
  title: "CoralSwarm",
  description: "Your team's shared knowledge, built from every source you connect.",
  // The product app is Clerk-gated on every host it answers for
  // (coralswarm.example, app./www., coralswarm-web.example) — nothing here,
  // including share links, should ever be indexed or diluting the
  // coralswarm.example marketing domain. Belt-and-braces with web/app/robots.ts
  // and the X-Robots-Tag response header in next.config.ts (standalone/Fly
  // mode only).
  robots: { index: false, follow: false },
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className={`${displayFont.variable} ${sans.variable} ${mono.variable}`}>
      <head>
        {/* Office.js nulls history.pushState/replaceState in Outlook desktop and the App Router needs them:
            repair + pin them before any Next chunk runs. No-op outside /outlook and /addin. See lib/addin/history.ts. */}
        <script dangerouslySetInnerHTML={{ __html: HISTORY_GUARD_SCRIPT }} />
      </head>
      <body>
        {/* One appearance for every Clerk surface (login, signup, settings)
            — see lib/clerkAppearance.ts. */}
        {/* Bundle Clerk's UI entry with Next so session initialization does
            not wait for a second CDN script/readiness poll. The actual UI
            remains available for login, signup and organization profiles. */}
        <ClerkProvider appearance={clerkAppearance} ui={ui} prefetchUI={false}>
          {/* <OrgHostSync> used to mount here, reconciling Clerk's active org
              with the page's hostname. Claim-based tenancy (PR 2.1) removed
              it: the tenant is named only by the JWT's `org_id` claim, so
              there is no second signal for the claim to disagree with. */}
          <AppProvider>{children}</AppProvider>
        </ClerkProvider>
      </body>
    </html>
  );
}
