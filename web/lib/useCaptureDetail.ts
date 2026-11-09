"use client";

import { useCallback, useRef } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useEvidenceAdmission } from "./useEvidenceAdmission";
import { pagePayloadQuery } from "./pageSections";
import { captureSectionParams, type CaptureDetailPayload, type CaptureSelectors } from "./pageSectionTypes/captureDetail";
import { refreshOwed, type Stored } from "./pageSectionWire";
import { StoredCertificateError } from "./pageSectionWire";
import { useReturnRefetch } from "./useReturnRefetch";

function captureAddress() {
  if (typeof window === "undefined") return "";
  const url = new URL(window.location.href);
  const selectors = new URLSearchParams();
  for (const key of ["graph_id", "source_id", "source_item_id", "version_id", "semantic_atom_id"]) {
    for (const value of url.searchParams.getAll(key)) selectors.append(key, value);
  }
  selectors.sort();
  return `${url.pathname}?${selectors}`;
}

/** Both meeting panels observe one indexed first page. Later evidence cursors
 * remain interactive reads, never additional model selectors. */
export function useCaptureDetail(selectors: CaptureSelectors, enabled = true) {
  const admission = useEvidenceAdmission();
  const address = captureAddress();
  const admittedToken = admission.getToken;
  const getToken = useCallback(() => {
    if (address !== captureAddress()) throw new StoredCertificateError("The selected capture address changed. Verify the current source.");
    return admittedToken();
  }, [address, admittedToken]);
  const client = useQueryClient();
  const options = pagePayloadQuery<CaptureDetailPayload>("capture_detail", admission.queryScope,
    captureSectionParams(selectors), getToken);
  const query = useQuery({ ...options, enabled: admission.ready && enabled });
  const raw = client.getQueryData<Stored<CaptureDetailPayload>>(options.queryKey);
  const lastRead = useRef(0);
  lastRead.current = Math.max(lastRead.current, query.dataUpdatedAt);
  const refetch = query.refetch;
  useReturnRefetch(useCallback(() => { void refetch({ cancelRefetch: false }); }, [refetch]), {
    enabled: admission.ready && enabled && !refreshOwed(raw), inFlight: () => query.isFetching,
    lastRunAtRef: lastRead, intervalMs: 60_000,
  });
  return { ...query, data: admission.ready && enabled ? query.data : undefined };
}
