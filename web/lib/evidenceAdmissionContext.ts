"use client";

import { createContext } from "react";
import type { Graph } from "./api";

export interface EvidenceAdmission {
  key: string;
  ready: boolean;
  getToken: () => string;
  graphs: Graph[];
  currentGraphId: string | null;
}
export const EvidenceAdmissionContext = createContext<EvidenceAdmission | null>(null);
