export const OPEN_INTENT_MS = 100;

/** Settle-before-act for a list of rows: `arm(id)` restarts one timer for the
 *  row under the pointer (or focus), `disarm()` cancels it, and `onFire` runs
 *  once for the row the user stayed on. A sweep down the list fires nothing. */
export function openIntent(onFire: (id: string) => void, delayMs = OPEN_INTENT_MS) {
  let timer: ReturnType<typeof setTimeout> | null = null;
  const disarm = () => {
    if (timer !== null) clearTimeout(timer);
    timer = null;
  };
  const arm = (id: string) => {
    disarm();
    timer = setTimeout(() => {
      timer = null;
      onFire(id);
    }, delayMs);
  };
  return { arm, disarm };
}
