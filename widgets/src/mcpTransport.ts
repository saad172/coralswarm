import type { Transport, JSONRPCMessage } from "@modelcontextprotocol/client";
import { JSONRPCMessageSchema } from "@modelcontextprotocol/core";

/** Referrers can name ChatGPT while the immediate parent is its sandbox
 * proxy. Trust the actual parent window, then pin its first valid message
 * origin. Opaque sandbox origins ("null") require wildcard outbound posts. */
export function acceptsParentMessage(event: MessageEvent, origin?: string): boolean {
  return event.source === window.parent && (origin === undefined || event.origin === origin);
}

/** SDK-compatible transport with source AND origin validation. All JSON-RPC
 * correlation, timeouts, initialization and notification schemas stay in
 * the official SDK instead of a second hand-written protocol client. */
export class ParentTransport implements Transport {
  onmessage?: Transport["onmessage"];
  onerror?: Transport["onerror"];
  onclose?: Transport["onclose"];
  private origin?: string;
  private listening = false;

  private receive = (event: MessageEvent) => {
    if (!acceptsParentMessage(event, this.origin)) return;
    const parsed = JSONRPCMessageSchema.safeParse(event.data);
    if (!parsed.success) return;
    this.origin ??= event.origin;
    this.onmessage?.(parsed.data);
  };

  async start() {
    if (this.listening) return;
    this.listening = true;
    window.addEventListener("message", this.receive);
  }

  async send(message: JSONRPCMessage) {
    window.parent.postMessage(message, this.origin && this.origin !== "null" ? this.origin : "*");
  }

  async close() {
    if (!this.listening) return;
    this.listening = false;
    window.removeEventListener("message", this.receive);
    this.onclose?.();
  }
}
