"use client";
import { Fragment, useCallback, useRef, type ReactNode } from "react";
import { useEvidenceAdmission } from "@/lib/useEvidenceAdmission";
import { EvidenceAdmissionContext } from "@/lib/evidenceAdmissionContext";
import { Loading } from "@/components/ui";
import { useCriticalPageReadLease } from "@/lib/pageReadRegistration";

/** Identity is frozen above the chat controller. Conversation/scope navigation
 * is separately fenced there, so selector changes preserve the unsent draft. */
export function ChatIdentityBoundary({ children }: { children: ReactNode }) {
  const admission = useEvidenceAdmission();
  useCriticalPageReadLease("chat identity verification", !admission.ready);
  const active = useRef(admission.key); active.current = admission.key;
  const admittedToken = admission.getToken;
  const key = admission.key;
  const getToken = useCallback(() => {
    if (active.current !== key) throw new Error("The chat account changed. Verify the current account and retry.");
    return admittedToken();
  }, [key, admittedToken]);
  if (!admission.ready) return <Loading what="Verifying chat account" />;
  return <EvidenceAdmissionContext.Provider value={{ ...admission, getToken }}><Fragment key={key}>{children}</Fragment></EvidenceAdmissionContext.Provider>;
}
