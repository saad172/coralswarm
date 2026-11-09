import { JEV_VERDICT_META, type JevVerdict, type VerdictTone } from "@/lib/graphSurfaces";

export interface VerdictCopy {
  label: string;
  glyph: string;
  tone: VerdictTone;
  description: string;
}

export function verdictCopy(verdict: JevVerdict | null | undefined): VerdictCopy | null {
  if (!verdict) return null;
  const meta = JEV_VERDICT_META[verdict];
  return { label: `Jev: ${meta.label}`, glyph: meta.glyph, tone: meta.tone, description: meta.description };
}

export function sourceButtonLabel(n: number, title: string, verdict: JevVerdict | null | undefined): string {
  const copy = verdictCopy(verdict);
  return `Open source ${n}: ${title}${copy ? ` (${copy.label})` : ""}`;
}
