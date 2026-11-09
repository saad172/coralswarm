import { apiRequest } from './api';
import { verifyPageReadToken, pageReadToken, terminalPageReadError } from './pageReadAuthority';
import { PageRefreshExpiredError, StoredRefreshCycle, refreshLimits } from './pageRefreshPolicy';
import { refreshRecoveryMs } from './pageRefreshPolicy';
import { pageSectionPath, typedStored, storedTimestamp, type PageModel, type SectionParams } from './pageSectionWire';

/** Interaction-only authoritative read after an action. The refresh POST is
 * normal; every owed poll remains critical and repeats the same indexed read.
 * Callers retain their action response until a real worker answer is ready. */
export async function refreshPageSection<P>(token: string, model: PageModel, params: SectionParams, signal?: AbortSignal, options: { onAcknowledged?: (deadline: number) => void; getToken?: () => string | null } = {}): Promise<P> {
  const acknowledgement = await apiRequest<{ refresh_eta: string | null }>(pageSectionPath(model, params, true), token, { method: 'POST' }, false, { signal, priority: 'normal' });
  if (!acknowledgement || !('refresh_eta' in acknowledgement) || !storedTimestamp(acknowledgement.refresh_eta)) throw new Error('Could not verify the refresh acknowledgement.');
  const deadline = Date.now() + refreshRecoveryMs(refreshLimits(model));
  options.onAcknowledged?.(deadline);
  let cycle: StoredRefreshCycle | undefined;
  while (true) {
    if (Date.now() >= deadline) throw new PageRefreshExpiredError();
    cycle?.beforeRead(Date.now());
    try {
      const stored = typedStored<P>(await apiRequest(pageSectionPath(model, params), token, undefined, false, { signal, priority: 'critical' }), model, params);
      if (signal?.aborted) throw signal.reason;
      if (options.getToken) verifyPageReadToken(token, await pageReadToken(options.getToken));
      if (Date.now() >= deadline) throw new PageRefreshExpiredError();
      if (stored.computed_at !== null && stored.refresh_eta === null) return stored.payload;
      // An action is waiting for fresh state even when an old answer is
      // usable for display. Keep its finite short-poll recovery cadence.
      cycle ??= new StoredRefreshCycle(refreshLimits(model), true);
      cycle.observe(stored, Date.now());
    } catch (error) {
      if (!cycle || terminalPageReadError(error) || error instanceof PageRefreshExpiredError || signal?.aborted) throw error;
      cycle.failure();
    }
    const delay = cycle.interval(Date.now());
    if (delay === false) throw new PageRefreshExpiredError();
    await new Promise<void>((resolve, reject) => {
      if (signal?.aborted) { reject(signal.reason); return; }
      const abort = () => { clearTimeout(timer); reject(signal?.reason); };
      const timer = setTimeout(() => { signal?.removeEventListener('abort', abort); resolve(); }, Math.min(delay, Math.max(1, deadline - Date.now())));
      signal?.addEventListener('abort', abort, { once: true });
    });
  }
}
