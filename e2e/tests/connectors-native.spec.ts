import { expect, test } from "../lib/fixtures";
import { personalUser } from "../lib/isolation";
import { API_BASE } from "../stack/config.mjs";
import { freshPayload } from "../lib/readModels";

// Real catalog/state/callback paths. Denial needs no upstream token exchange;
// successful native capture still requires provider-specific transport fakes.
for (const provider of ["google", "microsoft", "github"]) {
  test(`${provider}: denied native consent leaves every source disconnected`, async ({ request }) => {
    const { api } = await personalUser(request, `native-${provider}`);
    const start = await api.json("POST", `/v1/connectors/${provider}/connect`, { return_to: "wizard" });
    const url = new URL(start.authorize_url);
    expect(url.origin).toBe(API_BASE);
    const state = url.searchParams.get("state");
    expect(state).toBeTruthy();
    const response = await request.get(`${API_BASE}/v1/oauth/${provider}/callback?error=access_denied&state=${encodeURIComponent(state!)}`, { maxRedirects: 0 });
    expect(response.status()).toBe(307);
    expect(response.headers().location).toContain(`provider=${provider}&result=error&code=denied`);
    const detail = await freshPayload(api, `/v1/connectors/${provider}`);
    expect(detail.status).toBe("not_connected");
    expect(detail.sources_on).toBe(0);
    expect(detail.sources.every((s: any) => !s.enabled && s.status === "off")).toBe(true);
    expect((await api.call("DELETE", `/v1/connectors/${provider}`)).status()).toBe(404);
  });
}
