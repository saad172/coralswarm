"use client";

import Link from "next/link";
import { Button, ErrorNote } from "@/components/ui";
import { useSettingsRole } from "@/components/settings/useSettingsRole";
import { GENERIC_OUT_OF_CREDITS, isRefusalReason, paywallCopy, type PaywallAudience } from "./paywall";

/* The "out of credits" note shown wherever a 402 lands. With a known
   `refusal_reason` it says which limit was hit and offers the right next step
   for who is reading; without one (older backend) it keeps the original
   generic sentence. */

function SupportNote() {
  return (
    <ErrorNote>
      {GENERIC_OUT_OF_CREDITS} Email{" "}
      <a href="mailto:person@example.com" className="underline">
        person@example.com
      </a>{" "}
      to get more.
    </ErrorNote>
  );
}

export function PaywallCopyNote({ reason, audience }: { reason: string | null | undefined; audience: PaywallAudience }) {
  const copy = paywallCopy(reason, audience);
  if (!copy) return <SupportNote />;
  return (
    <ErrorNote
      action={
        copy.action ? (
          <Link href={copy.action.href}>
            <Button size="sm">{copy.action.label}</Button>
          </Link>
        ) : undefined
      }
    >
      {copy.message}
    </ErrorNote>
  );
}

function RoleAwareNote({ reason }: { reason: string }) {
  const { role, loaded } = useSettingsRole();
  // Until the role resolves, show the safest wording: it names no action the reader may not have.
  const audience: PaywallAudience = !loaded ? "member" : role === "org-member" ? "member" : role === "org-admin" ? "admin" : "personal";
  return <PaywallCopyNote reason={reason} audience={audience} />;
}

export function PaywallNote({ reason }: { reason?: string | null }) {
  if (!isRefusalReason(reason)) return <SupportNote />;
  return <RoleAwareNote reason={reason} />;
}
