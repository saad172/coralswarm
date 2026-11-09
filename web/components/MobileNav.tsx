"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { IconDots } from "./icons";
import { MAIN, UTILITIES, isActive, type NavItem } from "@/lib/nav";

/* Phone bottom tab bar. Hidden at lg+, where the 224px sidebar takes over.

   The redesign's places map onto a tab bar exactly, which is part of why
   the IA collapsed to five: the desktop rail and the phone bar can finally
   show the same thing. The sixth tab opens the More launcher, which holds the
   two utilities and the Library facets — everything that is a filter or a
   setting rather than a place. */

const MORE_TAB: NavItem = { href: "/more", label: "More", hue: "pink", Icon: IconDots };
const TABS: NavItem[] = [...MAIN, MORE_TAB];

/** Routes reached from the More launcher — keep More lit while inside them. */
const MORE_ROUTES = [MORE_TAB.href, ...UTILITIES.map((u) => u.href)];

export default function MobileNav() {
  const pathname = usePathname() ?? "";
  return (
    <nav
      aria-label="Primary"
      className="flex flex-shrink-0 border-t border-line lg:hidden"
      style={{
        height: "calc(64px + env(safe-area-inset-bottom))",
        paddingBottom: "env(safe-area-inset-bottom)",
        background: "color-mix(in srgb, var(--bg) 88%, transparent)",
        backdropFilter: "blur(12px)",
        WebkitBackdropFilter: "blur(12px)",
      }}
    >
      {TABS.map((t) => {
        const active =
          t.href === MORE_TAB.href
            ? MORE_ROUTES.some((r) => isActive(pathname, r))
            : isActive(pathname, t.href);
        return (
          <Link
            key={t.href}
            href={t.href}
            aria-current={active ? "page" : undefined}
            className="relative flex flex-1 flex-col items-center justify-center gap-1"
            style={{ color: active ? "var(--brand-2)" : "var(--ink-3)" }}
          >
            <t.Icon width={22} height={22} />
            <span
              className="font-mono text-[9.5px] tracking-[0.04em]"
              style={{ fontWeight: active ? 600 : 400 }}
            >
              {t.label}
            </span>
          </Link>
        );
      })}
    </nav>
  );
}
