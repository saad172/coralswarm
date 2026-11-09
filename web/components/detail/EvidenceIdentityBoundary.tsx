"use client";

import { Fragment, useCallback, useRef, type ReactNode } from "react";
import { useSearchParams } from "next/navigation";
import { useEvidenceAdmission } from "@/lib/useEvidenceAdmission";
import { EvidenceAdmissionContext } from "@/lib/evidenceAdmissionContext";
import { Loading } from "@/components/ui";
import { useCriticalPageReadLease } from "@/lib/pageReadRegistration";

/** Remount the whole detail subtree before prior title/metadata/overlay state
 * can paint under a new identity or selected object. No credential is a key. */
export function EvidenceIdentityBoundary({ children }: { children: ReactNode }) {
  const admission = useEvidenceAdmission();
  useCriticalPageReadLease("detail identity verification", !admission.ready);
  const params = useSearchParams();
  const object = JSON.stringify([params.get("id"), params.get("source_id"), params.get("source_item_id"), params.get("graph_id"), params.get("local"), params.get("window")]);
  const key = `${admission.key}:${object}`;
  const activeKey = useRef(key);
  activeKey.current = key;
  const admittedToken = admission.getToken;
  const getToken = useCallback(() => { if (activeKey.current !== key) throw new Error("The selected detail changed. Open the current item and retry."); return admittedToken(); }, [key, admittedToken]);
  if (!admission.ready) return <Loading what="Verifying detail account" />;
  return <EvidenceAdmissionContext.Provider value={{ ...admission, key, getToken }}><Fragment key={key}>{children}</Fragment></EvidenceAdmissionContext.Provider>;
}
