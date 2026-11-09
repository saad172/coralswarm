import type { LoadAttempt, OwedEvidence } from './pageLoadEvidence';
import type { PageModel } from './pageSectionWire';
export function recordPollTimingCandidate(owner: object, attempt: LoadAttempt, previous: LoadAttempt, model: PageModel, owedFrom: OwedEvidence[], minimumSpacingMs: number): void;
export function readPollTimingCandidate(owner: object, attempt: LoadAttempt, previous: LoadAttempt): { model: PageModel; minimumSpacingMs: number; owedFrom: OwedEvidence[] } | undefined;
