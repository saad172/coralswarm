/* Leaving for Stripe (Checkout or the billing portal) and coming back.
   Kept in one place so tests can stub the redirect. */

/** Send the person to a Stripe-hosted page, navigating in place. */
export async function goToStripe(url: string): Promise<void> {
  window.location.assign(url);
}

/** Absolute URL on this web app for Stripe to send people back to. */
export function returnUrl(path: string, query?: string): string {
  return `${window.location.origin}${path}${query ? `?${query}` : ""}`;
}
