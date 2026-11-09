import type { Metadata, Viewport } from "next";
import Script from "next/script";
import { Geist, Instrument_Serif, JetBrains_Mono } from "next/font/google";
import { VisitPing } from "@/components/VisitPing";
import "./globals.css";

/* Three families, self-hosted out of the static export by next/font.
   The design system's tokens/fonts.css pulled them from Google Fonts
   with a CSS @import — the slowest possible shape (a render-blocking
   stylesheet that has to resolve before the font requests even start)
   on a page whose LCP element is a clamp(44px,6.2vw,76px) serif. It
   also sent every visitor's IP to Google, which is a poor look
   directly above a band titled "Private by construction".

   Each font exposes the CSS variable that globals.css builds --display
   / --sans / --mono on top of. */

const display = Instrument_Serif({
  weight: "400",
  style: ["normal", "italic"],
  subsets: ["latin"],
  display: "swap",
  variable: "--font-display",
});

const sans = Geist({
  subsets: ["latin"],
  display: "swap",
  variable: "--font-sans",
});

const mono = JetBrains_Mono({
  subsets: ["latin"],
  display: "swap",
  variable: "--font-mono",
});

const SITE_URL = "https://coralswarm.example";
const TITLE = "CoralSwarm — organizational memory, automated";
const DESCRIPTION =
  "CoralSwarm captures the work your team is already doing — meetings, chat, email, coding sessions — splits it into atoms, groups them into topics, and answers questions with citations. For humans and AI.";

export const metadata: Metadata = {
  metadataBase: new URL(SITE_URL),
  title: TITLE,
  description: DESCRIPTION,
  alternates: { canonical: "/" },
  openGraph: {
    type: "website",
    url: SITE_URL,
    siteName: "CoralSwarm",
    title: TITLE,
    description: DESCRIPTION,
  },
  twitter: {
    card: "summary_large_image",
    site: "@CoralSwarmAi",
    title: TITLE,
    description: DESCRIPTION,
  },
};

// Separate from `metadata` per the Next 14+ split (themeColor moved out
// of the Metadata type). `--bg-2` is globals.css's actual `body`
// background (the cool-neutral canvas the 2026-07 redesign landed on,
// not the brand skill's warm-cream token) — see globals.css's header
// comment for why those two disagree and the token layer wins here.
export const viewport: Viewport = {
  themeColor: "#f5f6f8",
};

/* Cloudflare Web Analytics: cookieless, no consent banner, and it runs
   on the same account this Worker already deploys to. Stays off until
   NEXT_PUBLIC_CF_BEACON_TOKEN is set, so no beacon ships from a
   preview or a local build. */
const CF_BEACON_TOKEN = process.env.NEXT_PUBLIC_CF_BEACON_TOKEN;

/* Structured data (JSON-LD): Organization + SoftwareApplication.
 *
 * The two priced offers mirror components/SectionPricing.tsx's PLANS
 * array verbatim ($25/mo · personal pro, $100/mo · personal max) — if
 * those numbers change, change them here too. Enterprise is
 * deliberately NOT represented as an Offer: its price is "Talk to
 * sales", and schema.org's Offer.price is meant to be a real number: a
 * search engine that took a placeholder here at face value would show
 * a wrong price in a rich result, which is worse than showing none.
 */
const ORGANIZATION_JSON_LD = {
  "@context": "https://schema.org",
  "@type": "Organization",
  name: "CoralSwarm",
  url: SITE_URL,
  logo: `${SITE_URL}/coralswarm-logo.png`,
  description: DESCRIPTION,
  sameAs: ["https://example.com/profile", "https://www.linkedin.com/company/coralswarm/"],
};

// No SearchAction — the site has no search, and a SearchAction whose
// target doesn't resolve to real results is worse than omitting it.
const WEBSITE_JSON_LD = {
  "@context": "https://schema.org",
  "@type": "WebSite",
  name: "CoralSwarm",
  url: SITE_URL,
};

const SOFTWARE_APPLICATION_JSON_LD = {
  "@context": "https://schema.org",
  "@type": "SoftwareApplication",
  name: "CoralSwarm",
  url: SITE_URL,
  applicationCategory: "BusinessApplication",
  operatingSystem: "Web",
  description: DESCRIPTION,
  offers: [
    {
      "@type": "Offer",
      name: "Personal Pro",
      price: "25",
      priceCurrency: "USD",
      priceSpecification: {
        "@type": "UnitPriceSpecification",
        price: "25",
        priceCurrency: "USD",
        unitText: "MONTH",
      },
      url: `${SITE_URL}/#pricing`,
    },
    {
      "@type": "Offer",
      name: "Personal Max",
      price: "100",
      priceCurrency: "USD",
      priceSpecification: {
        "@type": "UnitPriceSpecification",
        price: "100",
        priceCurrency: "USD",
        unitText: "MONTH",
      },
      url: `${SITE_URL}/#pricing`,
    },
  ],
};

// `<` is escaped so a literal "</script>" can never appear inside the
// serialized JSON and prematurely close the tag this is injected into.
function jsonLd(data: unknown): string {
  return JSON.stringify(data).replace(/</g, "\\u003c");
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className={`${display.variable} ${sans.variable} ${mono.variable}`}>
      <body>
        <script
          type="application/ld+json"
          dangerouslySetInnerHTML={{ __html: jsonLd(ORGANIZATION_JSON_LD) }}
        />
        <script
          type="application/ld+json"
          dangerouslySetInnerHTML={{ __html: jsonLd(WEBSITE_JSON_LD) }}
        />
        <script
          type="application/ld+json"
          dangerouslySetInnerHTML={{ __html: jsonLd(SOFTWARE_APPLICATION_JSON_LD) }}
        />
        {/* Mounted ahead of {children} — see VisitPing.tsx's ordering note:
            its effect (a leaf) must commit before SectionContact's
            captureFirstTouch() call, deeper in {children}'s tree, or the
            first-visit check races the stash it's reading. */}
        <VisitPing />
        {children}
        {CF_BEACON_TOKEN && (
          <Script
            src="https://static.cloudflareinsights.com/beacon.min.js"
            strategy="afterInteractive"
            data-cf-beacon={JSON.stringify({ token: CF_BEACON_TOKEN })}
          />
        )}
      </body>
    </html>
  );
}
