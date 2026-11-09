"use client";

import Link from "next/link";
import { useUser } from "@clerk/nextjs";
import { useApp } from "@/lib/providers";
import { Avatar, Eyebrow } from "@/components/ui";
import { IconLogout } from "@/components/icons";
import { UTILITIES, ADD } from "@/lib/nav";
import { FALLBACK_NAME, accountLabel } from "@/lib/profile";

/* Mobile "More" launcher, reached from the bottom-nav More tab (< lg).

   Holds everything that is a setting rather than a place: the two utilities
   and Add sources.
   On desktop these live in the sidebar's sub-list and utility group, so this
   page is mobile-first but harmless at any width. Items come from the shared
   nav module so labels, icons and hues can't drift between surfaces. */

const ITEMS = [ADD, ...UTILITIES];

export default function MorePage() {
  const { logout } = useApp();
  const { user } = useUser();
  const email = user?.primaryEmailAddress?.emailAddress ?? null;
  // Same label derivation as /profile and the console header (lib/profile.ts).
  const name = accountLabel(
    { fullName: user?.fullName, username: user?.username, primaryEmail: email },
    FALLBACK_NAME,
  );
  return (
    <div className="mx-auto max-w-[560px]">
      {/* Profile — the mobile entry point into /profile (the console header's
          avatar is the desktop one). Read-only account view; sign-out stays the
          explicit button at the bottom of this page. */}
      <Link
        href="/profile"
        className="-mx-2 flex items-center gap-3 rounded-card px-[10px] pb-[18px] pt-[6px] hover:bg-bg-2"
      >
        <Avatar name={name} size={44} hue="coral" />
        <div className="min-w-0 flex-1">
          <div className="truncate text-[17px] font-medium text-ink">{name}</div>
          {email && email !== name && (
            <div className="truncate font-mono text-[11px] text-ink-3">{email}</div>
          )}
        </div>
        <span className="text-[18px] text-ink-3">›</span>
      </Link>

      <Eyebrow className="mb-[11px]">workspace</Eyebrow>
      <div className="overflow-hidden rounded-card border border-line bg-bg">
        {ITEMS.map((it, i) => (
          <Link
            key={it.href}
            href={it.href}
            className={`flex items-center gap-[13px] px-[15px] py-[14px] ${
              i ? "border-t border-line" : ""
            } hover:bg-bg-2`}
          >
            <span
              className="grid h-[34px] w-[34px] flex-shrink-0 place-items-center rounded-row"
              style={{
                background: `color-mix(in srgb, var(--hue-${it.hue}) 18%, var(--bg))`,
                color: `var(--hue-${it.hue})`,
              }}
            >
              <it.Icon width={18} height={18} />
            </span>
            <span className="flex-1">
              <span className="block text-[15px] text-ink">{it.label}</span>
              <span className="mt-[2px] block font-mono text-[10.5px] text-ink-3">{it.meta}</span>
            </span>
            <span className="text-[18px] text-ink-3">›</span>
          </Link>
        ))}
      </div>

      <button
        type="button"
        onClick={logout}
        className="mt-4 flex w-full items-center justify-center gap-[9px] rounded-pill border border-line bg-bg px-4 py-[13px] text-[13.5px] text-ink-2 hover:border-ink-3"
      >
        <IconLogout width={15} height={15} /> Sign out
      </button>
    </div>
  );
}
