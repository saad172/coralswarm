import { expect, type Page } from '@playwright/test';

/** Control delivery of the real backend's SSE, never its stages or answer.
 * A fast fake provider can put retrieved + writing in one network chunk;
 * React may batch them. Gate on DOM acknowledgement when testing each label,
 * and coalesce that pair explicitly when testing the same cited final answer. */
export async function controlAskProgress(page: Page, delivery: 'gated' | 'coalesced') {
  await page.exposeBinding('acknowledgeAskProgress', async (_source, stage: string) => {
    const label = { searching: 'Searching your workspaces', retrieved: 'Reading', writing: 'Writing answer', checking: 'Checking citations' }[stage];
    if (!label) throw new Error(`Unexpected Ask progress stage: ${stage}`);
    await expect(page.locator('[aria-live="polite"]').filter({ hasText: label })).toBeVisible();
  });
  await page.evaluate((delivery) => {
    const originalFetch = window.fetch.bind(window);
    const state = window as unknown as {
      askWireStages: string[];
      acknowledgeAskProgress: (stage: string) => Promise<void>;
    };
    state.askWireStages = [];
    window.fetch = async (...args) => {
      const response = await originalFetch(...args);
      if (!response.url.endsWith('/v1/queries/stream') || !response.ok || !response.body) return response;
      const reader = response.body.getReader(), decoder = new TextDecoder(), encoder = new TextEncoder();
      const body = new ReadableStream<Uint8Array>({
        async start(controller) {
          let buffer = '', held = '';
          try {
            for (;;) {
              const chunk = await reader.read();
              buffer += decoder.decode(chunk.value, { stream: !chunk.done });
              let end: number;
              while ((end = buffer.indexOf('\n\n')) !== -1) {
                const frame = buffer.slice(0, end + 2); buffer = buffer.slice(end + 2);
                const event = frame.split('\n').find((line) => line.startsWith('event:'))?.slice(6).trim();
                const data = frame.split('\n').filter((line) => line.startsWith('data:')).map((line) => line.slice(5)).join('\n');
                const stage: string | undefined = event === 'progress' ? JSON.parse(data).stage : undefined;
                if (stage) state.askWireStages.push(stage);
                if (delivery === 'coalesced' && stage === 'retrieved') { held += frame; continue; }
                controller.enqueue(encoder.encode(held + frame)); held = '';
                if (stage) await state.acknowledgeAskProgress(stage);
              }
              if (chunk.done) break;
            }
            if (held + buffer) controller.enqueue(encoder.encode(held + buffer));
            controller.close();
          } catch (error) { controller.error(error); }
          finally { reader.cancel().catch(() => {}); }
        },
        cancel(reason) { return reader.cancel(reason); },
      });
      return new Response(body, { status: response.status, statusText: response.statusText, headers: response.headers });
    };
  }, delivery);
}
