// Exact run only: never a global/stale sweep on the shared DEV instance.
import { cleanupE2eArtifacts, runId } from '../stack/clerk-cleanup.mjs';
if (!process.env.E2E_RUN_ID) throw new Error('explicit owned E2E_RUN_ID required');
const result = await cleanupE2eArtifacts({ prefix: `${runId()}-`, staleAfterMs: Infinity });
console.log(JSON.stringify({ prefix: `${runId()}-`, ...result, stale_sweep: false }));
