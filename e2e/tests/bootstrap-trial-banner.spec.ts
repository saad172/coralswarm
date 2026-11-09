import { expect } from '@playwright/test';
import { test, signIn, activateOrganization } from '../lib/fixtures';
import { createReadyOrg, personalUser } from '../lib/isolation';

for (const plane of ['personal', 'org'] as const) {
  test(`${plane}: trial-only boot keeps full Billing separate`, async ({ page, request }) => {
    test.setTimeout(120_000);
    const org = plane === 'org' ? await createReadyOrg(request, 'trial-banner') : null;
    const personal = org ? null : await personalUser(request, 'trial-banner-personal');
    const who = org?.admin ?? personal!;
    const api = org?.adminInOrg ?? who.api;
    const legacy = await api.json('GET', '/v1/bootstrap');
    const opted = await api.json('GET', '/v1/bootstrap?billing_banner=trial_v1');
    expect(opted.billing_banner_mode).toBe('trial_v1');
    expect(opted).not.toHaveProperty('billing_banner');
    expect(opted.me).toEqual(legacy.me);
    expect(opted.graphs).toEqual(legacy.graphs);
    expect(opted.trial_banner).toEqual(org ? { kind: 'trial_banner_v1', trial: legacy.billing_banner.trial } : null);

    await signIn(page, who.email);
    if (org) await activateOrganization(page, org.orgId);
    const reads: string[] = [];
    page.on('request', (req) => { if (req.method() === 'GET') reads.push(new URL(req.url()).pathname); });
    const bootResponse = page.waitForResponse((res) => new URL(res.url()).pathname === '/v1/bootstrap');
    await page.goto('/home');
    const boot = await bootResponse;
    expect(new URL(boot.url()).searchParams.get('billing_banner')).toBe('trial_v1');
    expect((await boot.json()).billing_banner_mode).toBe('trial_v1');
    await expect(page.locator('[data-page-settled="true"]').getByRole('main')).toBeVisible();
    expect(reads.filter((path) => path === '/v1/billing/summary' || path === '/v1/billing/trial-banner')).toEqual([]);
    if (org) await expect(page.getByText(/Free trial:.*credits left/)).toBeVisible();
    const billingResponse = page.waitForResponse((res) => new URL(res.url()).pathname === '/v1/billing/summary');
    await page.getByRole('link', { name: 'Settings', exact: true }).click();
    await page.locator(`a[href="${org ? '/settings/organization/billing' : '/settings/billing'}"]`).filter({ hasText: 'Billing' }).first().click();
    const summary = await (await billingResponse).json();
    for (const field of ['account_kind', 'plan', 'seats', 'credits', 'extra_usage', 'subscription', 'payment_method', 'trial']) {
      expect(summary).toHaveProperty(field);
    }
    expect(summary).not.toHaveProperty('kind');
    expect(summary.trial).toEqual(legacy.billing_banner?.trial ?? null);
  });
}
