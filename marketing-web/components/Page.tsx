"use client";

import { Hero } from "./Hero";
import { SectionAgents } from "./SectionAgents";
import { SectionAsk } from "./SectionAsk";
import { SectionContact } from "./SectionContact";
import { SectionFaq } from "./SectionFaq";
import { SectionHow } from "./SectionHow";
import { SectionPricing } from "./SectionPricing";
import { SiteFooter } from "./SiteFooter";
import { SiteNav } from "./SiteNav";
import { useReducedMotion, useReveal, useSmoothNav } from "./motion";

/* The whole marketing home — the v4a "safe cut" plus one later
   addition. Nine blocks, every link an in-page anchor.

   nav · hero (grey) · how (grey) · ask (white) · agents (ink) ·
   pricing (grey) · faq (white) · contact (white) · footer

   This is the shortened rewrite of v3: the scroll-driven hero is now
   one self-playing screen, v3's seven numbered explainers collapse into
   the three-card strip in `how`, ask and the oceans story share one
   section, and security folds into the pricing header. `faq` was added
   later, ahead of contact, for AEO/SEO — see SectionFaq.tsx. */

export function Page() {
  const reduced = useReducedMotion();
  useSmoothNav(reduced);
  useReveal(reduced);

  return (
    <>
      <SiteNav reduced={reduced} />
      <Hero reduced={reduced} />
      <SectionHow reduced={reduced} />
      <SectionAsk reduced={reduced} />
      <SectionAgents />
      <SectionPricing />
      <SectionFaq />
      <SectionContact />
      <SiteFooter />
    </>
  );
}
