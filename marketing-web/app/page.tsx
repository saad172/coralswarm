import { Page } from "@/components/Page";
import { FAQ_ITEMS } from "@/components/SectionFaq";

/* FAQPage JSON-LD lives here, not in layout.tsx with the site-wide
 * entity blocks: Google requires FAQPage structured data to mirror the
 * page's *visible* FAQ copy, and only the homepage renders SectionFaq —
 * emitting it from the layout would attach it to /privacy and /terms,
 * which have no FAQ. The Q/A text is FAQ_ITEMS itself (see that file's
 * header comment), so parity with the visible copy is enforced by
 * construction. */
const FAQ_JSON_LD = {
  "@context": "https://schema.org",
  "@type": "FAQPage",
  mainEntity: FAQ_ITEMS.map((item) => ({
    "@type": "Question",
    name: item.q,
    acceptedAnswer: {
      "@type": "Answer",
      text: item.a,
    },
  })),
};

// Same escaping rule as layout.tsx's jsonLd(): a literal "</script>"
// must never appear inside the serialized JSON.
const faqJsonLd = JSON.stringify(FAQ_JSON_LD).replace(/</g, "\\u003c");

export default function Home() {
  return (
    <>
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{ __html: faqJsonLd }}
      />
      <Page />
    </>
  );
}
