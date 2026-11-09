import { renderToStaticMarkup } from "react-dom/server";
import { afterEach, describe, expect, it, vi } from "vitest";

afterEach(() => { vi.unstubAllEnvs(); vi.resetModules(); });

// The layout reads NEXT_PUBLIC_API_BASE when it is first evaluated, and vitest has none: stub it BEFORE the
// import, otherwise these assertions would only ever see the "no env" branch.
async function render() {
  const { default: OutlookLayout } = await import("./layout");
  return renderToStaticMarkup(<OutlookLayout><main>pane</main></OutlookLayout>);
}

describe("outlook layout hints", () => {
  it("preloads office.js, connects to the API origin and resolves the Microsoft login host", async () => {
    vi.stubEnv("NEXT_PUBLIC_API_BASE", "https://api.coralswarm.example/");
    const html = await render();
    expect(html).toContain('rel="preload"'); expect(html).toContain('href="https://appsforoffice.microsoft.com/lib/1/hosted/office.js"'); expect(html).toContain('as="script"');
    expect(html).toContain('rel="preconnect"'); expect(html).toContain('href="https://api.coralswarm.example"');
    expect(html).toContain('rel="dns-prefetch"'); expect(html).toContain("https://login.microsoftonline.com");
    expect(html).toContain("<main>pane</main>");
  });

  it("omits the API hint rather than emitting a broken one when the base is unset or invalid", async () => {
    vi.stubEnv("NEXT_PUBLIC_API_BASE", "not a url");
    const html = await render();
    expect(html).not.toContain('rel="preconnect"');
    expect(html).toContain('rel="preload"');
  });
});
