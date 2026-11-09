import { vi } from "vitest";
import type { McpUiHostContext } from "@modelcontextprotocol/ext-apps";

interface Packet {
  jsonrpc: "2.0";
  id?: string | number;
  method?: string;
  params?: Record<string, unknown>;
  result?: unknown;
  error?: { code: number; message: string };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

/** Exercises the real SDK/transport, without mocking either of them. Only
 * the parent Window's postMessage endpoint is replaced by a host fixture. */
export function installMcpHost(options: {
  context?: McpUiHostContext;
  onInitialized?: () => void;
  callTool?: (name: string, args: Record<string, unknown>) => unknown | Promise<unknown>;
} = {}) {
  const frame = document.createElement("iframe");
  document.body.appendChild(frame);
  const parent = frame.contentWindow!;
  const previousParent = Object.getOwnPropertyDescriptor(window, "parent")!;
  Object.defineProperty(window, "parent", { configurable: true, value: parent });
  const packets: Packet[] = [];
  const targets: string[] = [];
  const origin = "https://host.example";
  const receive = (data: object, override: { source?: Window; origin?: string } = {}) => {
    window.dispatchEvent(new MessageEvent("message", {
      data: { jsonrpc: "2.0", ...data }, source: override.source ?? parent,
      origin: override.origin ?? origin,
    }));
  };
  const post = vi.spyOn(parent, "postMessage").mockImplementation((data, target) => {
    const packet = data as Packet;
    packets.push(packet);
    targets.push(typeof target === "string" ? target : "");
    // postMessage is asynchronous in real browsers, including its reply.
    queueMicrotask(() => {
      const reply = (result: unknown) => receive({ id: packet.id, result });
      const invalid = () => receive({ id: packet.id, error: { code: -32600, message: "Invalid request" } });
      switch (packet.method) {
        case "ui/initialize":
          if (typeof packet.params?.protocolVersion !== "string") { invalid(); break; }
          reply({ protocolVersion: packet.params?.protocolVersion, hostInfo: { name: "Protocol test", version: "1.0.0" }, hostCapabilities: { serverTools: {}, openLinks: {} }, hostContext: options.context ?? { theme: "light", displayMode: "inline" } });
          break;
        case "ui/notifications/initialized": options.onInitialized?.(); break;
        case "tools/call": {
          const name = packet.params?.name;
          const args = packet.params?.arguments;
          if (typeof name !== "string" || (args !== undefined && !isRecord(args))) { invalid(); break; }
          Promise.resolve(options.callTool?.(name, args ?? {})).then(
            (structuredContent) => reply({ content: [], structuredContent }),
            () => receive({ id: packet.id, error: { code: -32603, message: "Tool failed" } }),
          );
          break;
        }
        case "ui/request-display-mode": {
          const mode = packet.params?.mode;
          if (mode !== "inline" && mode !== "fullscreen" && mode !== "pip") { invalid(); break; }
          reply({ mode });
          break;
        }
        case "ui/open-link":
          if (typeof packet.params?.url !== "string") { invalid(); break; }
          reply({});
          break;
      }
    });
  });
  return {
    parent, packets, targets, receive,
    notify: (method: string, params: unknown) => receive({ method, params }),
    close: () => { post.mockRestore(); Object.defineProperty(window, "parent", previousParent); frame.remove(); },
  };
}
