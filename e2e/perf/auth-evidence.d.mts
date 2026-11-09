import type { LoadBudget } from '../lib/pageLoadBudget';
import type { DiscoveredRead, NetworkRecord } from './nightly';
import type { Page } from '@playwright/test';
export interface DocumentExpectation {
  access: 'public' | 'authenticated' | 'admin'; webBase: string;
  audience?: 'anonymous' | 'authenticated';
  sessionId?: string; userId?: string; orgId?: string | null; scope?: string;
  action?: LoadBudget['action']; expectedReads?: DiscoveredRead[];
}
export type DocumentOutcome = 'authenticated-document' | 'public-document' | 'auth-fatal' | 'destination-fatal' | 'unknown-fatal' | 'content-evidence-failure';
export interface DocumentEvidence {
  kind: DocumentOutcome; ok: boolean; fatal: boolean; reasons: string[];
  final_url: string | null; canonical_destination: string | null;
  browser: Record<string, boolean> | null; frames: unknown[];
  api_scope_matches_expected: boolean | null; compared_api_records: number;
}
export function createDocumentEvidence(page: Page, options: { requested: string; expected?: DocumentExpectation;
  resolveAlias: (path: string, search: string, context?: { audience?: 'anonymous' | 'authenticated' }) => string | null; now: () => number }): {
    complete(observation: { records: NetworkRecord[]; ledger: unknown[]; errors: string[] }): Promise<DocumentEvidence>;
  };
export function classifyDocument(input: unknown): DocumentEvidence;
export function safeDocumentUrl(value: unknown, base?: string): string | null;
